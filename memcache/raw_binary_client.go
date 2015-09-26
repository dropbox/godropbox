package memcache

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/dropbox/godropbox/errors"
)

const (
	headerLength = 24
	maxKeyLength = 250
	// NOTE: Storing values larger than 1MB requires recompiling memcached.
	maxValueLength = 1024 * 1024
)

func isValidKeyChar(char byte) bool {
	return (0x21 <= char && char <= 0x7e) || (0x80 <= char && char <= 0xff)
}

func isValidKeyString(key string) bool {
	if len(key) > maxKeyLength {
		return false
	}

	for _, char := range []byte(key) {
		if !isValidKeyChar(char) {
			return false
		}
	}

	return true
}

func validateValue(value []byte) error {
	if value == nil {
		return errors.New("Invalid value: cannot be nil")
	}

	if len(value) > maxValueLength {
		return errors.Newf(
			"Invalid value: length %d longer than max length %d",
			len(value),
			maxValueLength)
	}

	return nil
}

type header struct {
	Magic             uint8
	OpCode            uint8
	KeyLength         uint16
	ExtrasLength      uint8
	DataType          uint8
	VBucketIdOrStatus uint16 // vbucket id for request, status for response
	TotalBodyLength   uint32
	Opaque            uint32 // unless value
	DataVersionId     uint64 // aka CAS
}

// An unsharded memcache client implementation which operates on a pre-existing
// io channel (The user must explicitly setup and close down the channel),
// using the binary memcached protocol.  Note that the client assumes nothing
// else is sending or receiving on the network channel.  In general, all client
// operations are serialized (Use multiple channels / clients if parallelism
// is needed).
type RawBinaryClient struct {
	shard      int
	channel    io.ReadWriter
	mutex      sync.Mutex
	validState bool
}

// This creates a new memcache RawBinaryClient.
func NewRawBinaryClient(shard int, channel io.ReadWriter) ClientShard {
	return &RawBinaryClient{
		shard:      shard,
		channel:    channel,
		validState: true,
	}
}

// See ClientShard interface for documentation.
func (c *RawBinaryClient) ShardId() int {
	return c.shard
}

// See ClientShard interface for documentation.
func (c *RawBinaryClient) IsValidState() bool {
	return c.validState
}

// Sends a memcache request through the connection.  NOTE: extras must be
// fix-sized values.
func (c *RawBinaryClient) sendRequest(
	code opCode,
	dataVersionId uint64, // aka CAS
	key []byte, // may be nil
	value []byte, // may be nil
	extras ...interface{}) (err error) {

	if !c.validState {
		// An error has occurred previously.  It's not safe to continue sending.
		return errors.New("Skipping due to previous error")
	}
	defer func() {
		if err != nil {
			c.validState = false
		}
	}()

	extrasBuffer := new(bytes.Buffer)
	for _, extra := range extras {
		err := binary.Write(extrasBuffer, binary.BigEndian, extra)
		if err != nil {
			return errors.Wrap(err, "Failed to write extra")
		}
	}

	// NOTE:
	// - memcache only supports a single dataType (0x0)
	// - vbucket id is not used by the library since vbucket related op
	//   codes are unsupported
	hdr := header{
		Magic:           reqMagicByte,
		OpCode:          byte(code),
		KeyLength:       uint16(len(key)),
		ExtrasLength:    uint8(extrasBuffer.Len()),
		TotalBodyLength: uint32(len(key) + len(value) + extrasBuffer.Len()),
		DataVersionId:   dataVersionId,
	}

	msgBuffer := new(bytes.Buffer)

	if err := binary.Write(msgBuffer, binary.BigEndian, hdr); err != nil {
		return errors.Wrap(err, "Failed to write header")
	}
	if msgBuffer.Len() != headerLength { // sanity check
		return errors.Newf("Incorrect header size: %d", msgBuffer.Len())
	}

	bytesWritten, err := extrasBuffer.WriteTo(msgBuffer)
	if err != nil {
		return errors.Wrap(err, "Failed to add extras to msg")
	}
	if bytesWritten != int64(hdr.ExtrasLength) {
		return errors.New("Failed to write out extras")
	}

	if key != nil {
		if _, err := msgBuffer.Write(key); err != nil {
			return errors.Wrap(err, "Failed to write key")
		}
	}

	if value != nil {
		if _, err := msgBuffer.Write(value); err != nil {
			return errors.Wrap(err, "Failed to write value")
		}
	}

	bytesWritten, err = msgBuffer.WriteTo(c.channel)
	if err != nil {
		return errors.Wrap(err, "Failed to send msg")
	}
	if bytesWritten != int64((hdr.TotalBodyLength)+headerLength) {
		return errors.New("Failed to sent out message")
	}

	return nil
}

// Receive a memcache response from the connection.  The status,
// dataVersionId (aka CAS), key and value are returned, while the extra
// values are stored in the arguments.  NOTE: extras must be pointers to
// fix-sized values.
func (c *RawBinaryClient) receiveResponse(
	expectedCode opCode,
	extras ...interface{}) (
	status ResponseStatus,
	dataVersionId uint64,
	key []byte, // is nil when key length is zero
	value []byte, // is nil when the value length is zero
	err error) {

	if !c.validState {
		// An error has occurred previously.  It's not safe to continue sending.
		err = errors.New("Skipping due to previous error")
		return
	}
	defer func() {
		if err != nil {
			c.validState = false
		}
	}()

	hdr := header{}
	if err = binary.Read(c.channel, binary.BigEndian, &hdr); err != nil {
		err = errors.Wrap(err, "Failed to read header")
		return
	}
	if hdr.Magic != respMagicByte {
		err = errors.Newf("Invalid response magic byte: %d", hdr.Magic)
		return
	}
	if hdr.OpCode != byte(expectedCode) {
		err = errors.Newf("Invalid response op code: %d", hdr.OpCode)
		return
	}
	if hdr.DataType != 0 {
		err = errors.Newf("Invalid data type: %d", hdr.DataType)
		return
	}

	valueLength := int(hdr.TotalBodyLength)
	valueLength -= (int(hdr.KeyLength) + int(hdr.ExtrasLength))
	if valueLength < 0 {
		err = errors.Newf("Invalid response header.  Wrong payload size.")
		return
	}

	status = ResponseStatus(hdr.VBucketIdOrStatus)
	dataVersionId = hdr.DataVersionId

	if hdr.ExtrasLength == 0 {
		if status == StatusNoError && len(extras) != 0 {
			err = errors.Newf("Expecting extras payload")
			return
		}
		// the response has no extras
	} else {
		extrasBytes := make([]byte, hdr.ExtrasLength, hdr.ExtrasLength)
		if _, err = io.ReadFull(c.channel, extrasBytes); err != nil {
			err = errors.Wrap(err, "Failed to read extra")
			return
		}

		extrasBuffer := bytes.NewBuffer(extrasBytes)

		for _, extra := range extras {
			err = binary.Read(extrasBuffer, binary.BigEndian, extra)
			if err != nil {
				err = errors.Wrap(err, "Failed to deserialize extra")
				return
			}
		}

		if extrasBuffer.Len() != 0 {
			err = errors.Newf("Not all bytes are consumed by extras fields")
			return
		}
	}

	if hdr.KeyLength > 0 {
		key = make([]byte, hdr.KeyLength, hdr.KeyLength)
		if _, err = io.ReadFull(c.channel, key); err != nil {
			err = errors.Wrap(err, "Failed to read key")
			return
		}
	}

	if valueLength > 0 {
		value = make([]byte, valueLength, valueLength)
		if _, err = io.ReadFull(c.channel, value); err != nil {
			err = errors.Wrap(err, "Failed to read value")
			return
		}
	}

	return
}

func (c *RawBinaryClient) sendGetRequest(key string) GetResponse {
	if !isValidKeyString(key) {
		return NewGetErrorResponse(
			key,
			errors.New("Invalid key"))
	}

	err := c.sendRequest(opGet, 0, []byte(key), nil)
	if err != nil {
		return NewGetErrorResponse(key, err)
	}

	return nil
}

func (c *RawBinaryClient) receiveGetResponse(key string) GetResponse {
	var flags uint32
	status, version, _, value, err := c.receiveResponse(opGet, &flags)
	if err != nil {
		return NewGetErrorResponse(key, err)
	}
	return NewGetResponse(key, status, flags, value, version)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Get(key string) GetResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if resp := c.sendGetRequest(key); resp != nil {
		return resp
	}

	return c.receiveGetResponse(key)
}

func (c *RawBinaryClient) removeDuplicateKey(keys []string) []string {
	keyMap := make(map[string]interface{})
	for _, key := range keys {
		keyMap[key] = nil
	}
	cacheKeys := make([]string, len(keyMap))
	i := 0
	for key, _ := range keyMap {
		cacheKeys[i] = key
		i = i + 1
	}
	return cacheKeys
}

// See Client interface for documentation.
func (c *RawBinaryClient) GetMulti(keys []string) map[string]GetResponse {
	if keys == nil {
		return nil
	}

	responses := make(map[string]GetResponse)
	cacheKeys := c.removeDuplicateKey(keys)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, key := range cacheKeys {
		if resp := c.sendGetRequest(key); resp != nil {
			responses[key] = resp
		}
	}

	for _, key := range cacheKeys {
		if _, inMap := responses[key]; inMap { // error occurred while sending
			continue
		}
		responses[key] = c.receiveGetResponse(key)
	}

	return responses
}

func (c *RawBinaryClient) sendMutateRequest(
	code opCode,
	item *Item,
	addExtras bool) MutateResponse {

	if item == nil {
		return NewMutateErrorResponse("", errors.New("item is nil"))
	}

	if !isValidKeyString(item.Key) {
		return NewMutateErrorResponse(
			item.Key,
			errors.New("Invalid key"))
	}

	if err := validateValue(item.Value); err != nil {
		return NewMutateErrorResponse(item.Key, err)
	}

	extras := make([]interface{}, 0, 2)
	if addExtras {
		extras = append(extras, item.Flags)
		extras = append(extras, item.Expiration)
	}

	err := c.sendRequest(
		code,
		item.DataVersionId,
		[]byte(item.Key),
		item.Value,
		extras...)
	if err != nil {
		return NewMutateErrorResponse(item.Key, err)
	}
	return nil
}

func (c *RawBinaryClient) receiveMutateResponse(
	code opCode,
	key string) MutateResponse {

	status, version, _, _, err := c.receiveResponse(code)
	if err != nil {
		return NewMutateErrorResponse(key, err)
	}
	return NewMutateResponse(key, status, version, false)
}

// Perform a mutation operation specified by the given code.
func (c *RawBinaryClient) mutate(code opCode, item *Item) MutateResponse {
	if item == nil {
		return NewMutateErrorResponse("", errors.New("item is nil"))
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if resp := c.sendMutateRequest(code, item, true); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(code, item.Key)
}

// Batch version of the mutate method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering)
func (c *RawBinaryClient) mutateMulti(
	code opCode,
	items []*Item) []MutateResponse {

	if items == nil {
		return nil
	}

	responses := make([]MutateResponse, len(items), len(items))

	// Short-circuit function to avoid locking.
	if len(items) == 0 {
		return responses
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, item := range items {
		responses[i] = c.sendMutateRequest(code, item, true)
	}

	for i, item := range items {
		if responses[i] != nil { // error occurred while sending
			continue
		}
		responses[i] = c.receiveMutateResponse(code, item.Key)
	}

	return responses
}

// See Client interface for documentation.
func (c *RawBinaryClient) Set(item *Item) MutateResponse {
	return c.mutate(opSet, item)
}

// See Client interface for documentation.
func (c *RawBinaryClient) SetMulti(items []*Item) []MutateResponse {
	return c.mutateMulti(opSet, items)
}

// See Client interface for documentation.
func (c *RawBinaryClient) SetSentinels(items []*Item) []MutateResponse {
	// For raw clients, there are no difference between SetMulti and
	// SetSentinels.
	return c.SetMulti(items)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Add(item *Item) MutateResponse {
	return c.mutate(opAdd, item)
}

// See Client interface for documentation.
func (c *RawBinaryClient) AddMulti(items []*Item) []MutateResponse {
	return c.mutateMulti(opAdd, items)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Replace(item *Item) MutateResponse {
	if item == nil {
		return NewMutateErrorResponse("", errors.New("item is nil"))
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if resp := c.sendMutateRequest(opReplace, item, true); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opReplace, item.Key)
}

func (c *RawBinaryClient) sendDeleteRequest(key string) MutateResponse {
	if !isValidKeyString(key) {
		return NewMutateErrorResponse(
			key,
			errors.New("Invalid key"))
	}

	if err := c.sendRequest(opDelete, 0, []byte(key), nil); err != nil {
		return NewMutateErrorResponse(key, err)
	}
	return nil
}

// See Client interface for documentation.
func (c *RawBinaryClient) Delete(key string) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if resp := c.sendDeleteRequest(key); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opDelete, key)
}

// See Client interface for documentation.
func (c *RawBinaryClient) DeleteMulti(keys []string) []MutateResponse {
	if keys == nil {
		return nil
	}

	responses := make([]MutateResponse, len(keys), len(keys))

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, key := range keys {
		responses[i] = c.sendDeleteRequest(key)
	}

	for i, key := range keys {
		if responses[i] != nil { // error occurred while sending
			continue
		}
		responses[i] = c.receiveMutateResponse(opDelete, key)
	}

	return responses
}

// See Client interface for documentation.
func (c *RawBinaryClient) Append(key string, value []byte) MutateResponse {
	item := &Item{
		Key:   key,
		Value: value,
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if resp := c.sendMutateRequest(opAppend, item, false); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opAppend, item.Key)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Prepend(key string, value []byte) MutateResponse {
	item := &Item{
		Key:   key,
		Value: value,
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if resp := c.sendMutateRequest(opPrepend, item, false); resp != nil {
		return resp
	}

	return c.receiveMutateResponse(opPrepend, item.Key)
}

func (c *RawBinaryClient) sendCountRequest(
	code opCode,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	if !isValidKeyString(key) {
		return NewCountErrorResponse(
			key,
			errors.New("Invalid key"))
	}

	err := c.sendRequest(
		code,
		0,
		[]byte(key),
		nil,
		delta,
		initValue,
		expiration)
	if err != nil {
		return NewCountErrorResponse(key, err)
	}
	return nil
}

func (c *RawBinaryClient) receiveCountResponse(
	code opCode,
	key string) CountResponse {

	status, _, _, value, err := c.receiveResponse(code)
	if err != nil {
		return NewCountErrorResponse(key, err)
	}

	valueBuffer := bytes.NewBuffer(value)
	var count uint64
	if err := binary.Read(valueBuffer, binary.BigEndian, &count); err != nil {
		return NewCountErrorResponse(key, err)
	}

	return NewCountResponse(key, status, count)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Increment(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	resp := c.sendCountRequest(opIncrement, key, delta, initValue, expiration)
	if resp != nil {
		return resp
	}
	return c.receiveCountResponse(opIncrement, key)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Decrement(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	resp := c.sendCountRequest(opDecrement, key, delta, initValue, expiration)
	if resp != nil {
		return resp
	}
	return c.receiveCountResponse(opDecrement, key)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Stat(statsKey string) StatResponse {
	shardEntries := make(map[int](map[string]string))
	entries := make(map[string]string)
	shardEntries[c.ShardId()] = entries

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !isValidKeyString(statsKey) {
		return NewStatErrorResponse(
			errors.Newf("Invalid key: %s", statsKey),
			shardEntries)
	}

	err := c.sendRequest(opStat, 0, []byte(statsKey), nil)
	if err != nil {
		return NewStatErrorResponse(err, shardEntries)
	}

	for true {
		status, _, key, value, err := c.receiveResponse(opStat)
		if err != nil {
			return NewStatErrorResponse(err, shardEntries)
		}
		if status != StatusNoError {
			// In theory, this is a valid state, but treating this as valid
			// complicates the code even more.
			c.validState = false
			return NewStatResponse(status, shardEntries)
		}
		if key == nil && value == nil { // the last entry
			break
		}
		entries[string(key)] = string(value)
	}
	return NewStatResponse(StatusNoError, shardEntries)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Version() VersionResponse {
	versions := make(map[int]string)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.sendRequest(opVersion, 0, nil, nil)
	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	status, _, _, value, err := c.receiveResponse(opVersion)
	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	versions[c.ShardId()] = string(value)
	return NewVersionResponse(status, versions)
}

func (c *RawBinaryClient) genericOp(
	code opCode,
	extras ...interface{}) Response {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.sendRequest(code, 0, nil, nil, extras...)
	if err != nil {
		return NewErrorResponse(err)
	}

	status, _, _, _, err := c.receiveResponse(code)
	if err != nil {
		return NewErrorResponse(err)
	}
	return NewResponse(status)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Flush(expiration uint32) Response {
	return c.genericOp(opFlush, expiration)
}

// See Client interface for documentation.
func (c *RawBinaryClient) Verbosity(verbosity uint32) Response {
	return c.genericOp(opVerbosity, verbosity)
}
