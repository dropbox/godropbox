package memcache

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/dropbox/godropbox/errors"
)

// An unsharded memcache client implementation which operates on a pre-existing
// io channel (The user must explicitly setup and close down the channel),
// using the ascii memcache protocol.  Note that the client assumes nothing
// else is sending or receiving on the network channel.  In general, all client
// operations are serialized (Use multiple channels / clients if parallelism
// is needed).
type RawAsciiClient struct {
	shard   int
	channel io.ReadWriter

	mutex      sync.Mutex
	validState bool
	writer     *bufio.Writer
	reader     *bufio.Reader
}

// This creates a new memcache RawAsciiClient.
func NewRawAsciiClient(shard int, channel io.ReadWriter) ClientShard {
	return &RawAsciiClient{
		shard:      shard,
		channel:    channel,
		validState: true,
		writer:     bufio.NewWriter(channel),
		reader:     bufio.NewReader(channel),
	}
}

func (c *RawAsciiClient) writeStrings(strs ...string) error {
	if !c.validState {
		return errors.New("Skipping due to previous error")
	}

	for _, str := range strs {
		_, err := c.writer.WriteString(str)
		if err != nil {
			c.validState = false
			return err
		}
	}

	return nil
}

func (c *RawAsciiClient) flushWriter() error {
	if !c.validState {
		return errors.New("Skipping due to previous error")
	}

	err := c.writer.Flush()
	if err != nil {
		c.validState = false
		return err
	}

	return nil
}

func (c *RawAsciiClient) readLine() (string, error) {
	line, isPrefix, err := c.reader.ReadLine()
	if err != nil {
		c.validState = false
		return "", err
	}
	if isPrefix {
		c.validState = false
		return "", errors.New("Readline truncated")
	}

	return string(line), nil
}

func (c *RawAsciiClient) read(numBytes int) ([]byte, error) {
	result := make([]byte, numBytes, numBytes)

	_, err := io.ReadFull(c.reader, result)
	if err != nil {
		c.validState = false
		return nil, err
	}

	return result, nil
}

func (c *RawAsciiClient) checkEmptyBuffers() error {
	if c.writer.Buffered() != 0 {
		c.validState = false
		return errors.New("writer buffer not fully flushed")
	}
	if c.reader.Buffered() != 0 {
		c.validState = false
		return errors.New("reader buffer not fully drained")
	}

	return nil
}

func (c *RawAsciiClient) ShardId() int {
	return c.shard
}

func (c *RawAsciiClient) IsValidState() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.validState
}

func (c *RawAsciiClient) Get(key string) GetResponse {
	return c.GetMulti([]string{key})[key]
}

func (c *RawAsciiClient) GetMulti(keys []string) map[string]GetResponse {
	responses := make(map[string]GetResponse, len(keys))
	neededKeys := []string{}
	for _, key := range keys {
		if _, ok := responses[key]; ok {
			continue
		}

		if !isValidKeyString(key) {
			responses[key] = NewGetErrorResponse(key, errors.New("Invalid key"))
			continue
		}

		neededKeys = append(neededKeys, key)
		responses[key] = nil
	}

	if len(neededKeys) == 0 {
		return responses
	}

	populateErrorResponses := func(e error) {
		for _, key := range neededKeys {
			if responses[key] == nil {
				responses[key] = NewGetErrorResponse(key, e)
			}
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// NOTE: Always use gets instead of get since returning the extra cas id
	// info is relatively cheap.
	err := c.writeStrings("gets")
	if err != nil {
		populateErrorResponses(err)
		return responses
	}

	for _, key := range neededKeys {
		err := c.writeStrings(" ", key)
		if err != nil {
			populateErrorResponses(err)
			return responses
		}
	}

	err = c.writeStrings("\r\n")
	if err != nil {
		populateErrorResponses(err)
		return responses
	}

	err = c.flushWriter()
	if err != nil {
		populateErrorResponses(err)
		return responses
	}

	// Any error that occurs while reading the results will result in mid
	// stream termination, i.e., the channel is no longer in valid state.
	for {
		line, err := c.readLine()
		if err != nil {
			populateErrorResponses(err)
			return responses
		}

		if line == "END" {
			break
		}

		slice := strings.Split(line, " ")

		// line is of the form: VALUE <key> <flag> <num bytes> <cas id>
		if len(slice) != 5 || slice[0] != "VALUE" {
			c.validState = false
			populateErrorResponses(errors.New(line))
			return responses
		}

		key := slice[1]
		if v, ok := responses[key]; !ok || v != nil {
			c.validState = false
			populateErrorResponses(errors.New(line))
			return responses
		}

		flags, err := strconv.ParseUint(slice[2], 10, 32)
		if err != nil {
			c.validState = false
			populateErrorResponses(errors.New(line))
			return responses
		}

		size, err := strconv.ParseUint(slice[3], 10, 32)
		if err != nil {
			c.validState = false
			populateErrorResponses(errors.New(line))
			return responses
		}

		version, err := strconv.ParseUint(slice[4], 10, 64)
		if err != nil {
			c.validState = false
			populateErrorResponses(errors.New(line))
			return responses
		}

		value, err := c.read(int(size) + 2)
		if err != nil {
			populateErrorResponses(err)
			return responses
		}

		if value[size] != '\r' && value[size+1] != '\n' {
			// sanity check
			populateErrorResponses(errors.New("Corrupted stream"))
			return responses
		}
		value = value[:size]

		// TODO(patrick): check status
		responses[key] = NewGetResponse(
			key,
			StatusNoError,
			uint32(flags),
			value,
			version)
	}

	err = c.checkEmptyBuffers()
	if err != nil {
		populateErrorResponses(err)
		return responses
	}

	for _, key := range neededKeys {
		if responses[key] == nil {
			responses[key] = NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
		}
	}

	return responses
}

func (c *RawAsciiClient) storeRequests(
	cmd string,
	items []*Item) []MutateResponse {

	var err error
	responses := make([]MutateResponse, len(items), len(items))
	needSending := false
	for i, item := range items {
		if item == nil {
			responses[i] = NewMutateErrorResponse("", errors.New("item is nil"))
			continue
		}

		if item.DataVersionId != 0 && cmd != "set" {
			responses[i] = NewMutateErrorResponse(
				item.Key,
				errors.Newf(
					"Ascii protocol does not support %s with cas id",
					cmd))
			continue
		}

		if !isValidKeyString(item.Key) {
			responses[i] = NewMutateErrorResponse(
				item.Key,
				errors.New("Invalid key"))
			continue
		}

		err = validateValue(item.Value)
		if err != nil {
			responses[i] = NewMutateErrorResponse(item.Key, err)
			continue
		}

		needSending = true
	}

	if !needSending {
		return responses
	}

	populateErrorResponses := func(e error) {
		for i, item := range items {
			if responses[i] == nil {
				responses[i] = NewMutateErrorResponse(item.Key, e)
			}
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// NOTE: store requests are pipelined.
	for i, item := range items {
		if responses[i] != nil {
			continue
		}

		flags := strconv.FormatUint(uint64(item.Flags), 10)
		expiration := strconv.FormatUint(uint64(item.Expiration), 10)
		size := strconv.Itoa(len(item.Value))

		if item.DataVersionId != 0 {
			// We have already verified that cmd must be "set"
			err = c.writeStrings(
				"cas ",
				item.Key, " ",
				flags, " ",
				expiration, " ",
				size, " ",
				strconv.FormatUint(item.DataVersionId, 10),
				"\r\n")
		} else {
			err = c.writeStrings(
				cmd, " ",
				item.Key, " ",
				flags, " ",
				expiration, " ",
				size,
				"\r\n")
		}

		if err != nil {
			populateErrorResponses(err)
			return responses
		}

		err = c.writeStrings(string(item.Value), "\r\n")
		if err != nil {
			populateErrorResponses(err)
			return responses
		}
	}

	err = c.flushWriter()
	if err != nil {
		populateErrorResponses(err)
		return responses
	}

	for i, item := range items {
		if responses[i] != nil {
			continue
		}

		line, err := c.readLine()
		if err != nil {
			populateErrorResponses(err)
			return responses
		}

		// NOTE: Unfortunately, the returned response does not include
		// cas info.
		if line == "STORED" {
			responses[i] = NewMutateResponse(
				item.Key,
				StatusNoError,
				0,
				true)
		} else if line == "NOT_FOUND" {
			responses[i] = NewMutateResponse(
				item.Key,
				StatusKeyNotFound,
				0,
				true)
		} else if line == "NOT_STORED" {
			responses[i] = NewMutateResponse(
				item.Key,
				StatusItemNotStored,
				0,
				true)
		} else if line == "EXISTS" {
			responses[i] = NewMutateResponse(
				item.Key,
				StatusKeyExists,
				0,
				true)
		} else {
			responses[i] = NewMutateErrorResponse(item.Key, errors.New(line))
		}
	}

	_ = c.checkEmptyBuffers()

	return responses
}

func (c *RawAsciiClient) Set(item *Item) MutateResponse {
	return c.SetMulti([]*Item{item})[0]
}

func (c *RawAsciiClient) SetMulti(items []*Item) []MutateResponse {
	return c.storeRequests("set", items)
}

func (c *RawAsciiClient) SetSentinels(items []*Item) []MutateResponse {
	// There are no difference between SetMutli and SetSentinels since
	// SetMulti issues set / cas commands depending on the items' version ids.
	return c.SetMulti(items)
}

func (c *RawAsciiClient) Add(item *Item) MutateResponse {
	return c.AddMulti([]*Item{item})[0]
}

func (c *RawAsciiClient) AddMulti(items []*Item) []MutateResponse {
	return c.storeRequests("add", items)
}

func (c *RawAsciiClient) Replace(item *Item) MutateResponse {
	return c.storeRequests("replace", []*Item{item})[0]
}

func (c *RawAsciiClient) Append(key string, value []byte) MutateResponse {
	items := []*Item{
		&Item{
			Key:   key,
			Value: value,
		},
	}
	return c.storeRequests("append", items)[0]
}

func (c *RawAsciiClient) Prepend(key string, value []byte) MutateResponse {
	items := []*Item{
		&Item{
			Key:   key,
			Value: value,
		},
	}
	return c.storeRequests("prepend", items)[0]
}

func (c *RawAsciiClient) Delete(key string) MutateResponse {
	return c.DeleteMulti([]string{key})[0]
}

func (c *RawAsciiClient) DeleteMulti(keys []string) []MutateResponse {
	responses := make([]MutateResponse, len(keys), len(keys))

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// NOTE: delete requests are pipelined.
	for i, key := range keys {
		if !isValidKeyString(key) {
			responses[i] = NewMutateErrorResponse(
				key,
				errors.New("Invalid key"))
			continue
		}

		err := c.writeStrings("delete ", key, "\r\n")
		if err != nil {
			responses[i] = NewMutateErrorResponse(key, err)
		}
	}

	err := c.flushWriter()
	if err != nil {
		// The delete requests may or may not have successfully reached the
		// memcached, just error out.
		for i, key := range keys {
			if responses[i] == nil {
				responses[i] = NewMutateErrorResponse(key, err)
			}
		}
	}

	for i, key := range keys {
		if responses[i] != nil {
			continue
		}

		line, err := c.readLine()
		if err != nil {
			responses[i] = NewMutateErrorResponse(key, err)
			continue
		}

		if line == "DELETED" {
			responses[i] = NewMutateResponse(key, StatusNoError, 0, true)
		} else if line == "NOT_FOUND" {
			responses[i] = NewMutateResponse(key, StatusKeyNotFound, 0, true)
		} else { // Unexpected error msg
			responses[i] = NewMutateErrorResponse(key, errors.New(line))
		}
	}

	_ = c.checkEmptyBuffers()

	return responses
}

func (c *RawAsciiClient) countRequest(
	cmd string,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	if expiration != 0xffffffff {
		return NewCountErrorResponse(
			key,
			errors.New(
				"Ascii protocol does not support initial value / "+
					"expiration.  expiration must be set to 0xffffffff."))
	}

	if !isValidKeyString(key) {
		return NewCountErrorResponse(
			key,
			errors.New("Invalid key"))
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.writeStrings(
		cmd, " ",
		key, " ",
		strconv.FormatUint(delta, 10), "\r\n")
	if err != nil {
		return NewCountErrorResponse(key, err)
	}

	err = c.flushWriter()
	if err != nil {
		return NewCountErrorResponse(key, err)
	}

	line, err := c.readLine()
	if err != nil {
		return NewCountErrorResponse(key, err)
	}

	_ = c.checkEmptyBuffers()

	if line == "NOT_FOUND" {
		return NewCountResponse(key, StatusKeyNotFound, 0)
	}

	val, err := strconv.ParseUint(line, 10, 64)
	if err != nil {
		return NewCountErrorResponse(key, err)
	}

	return NewCountResponse(key, StatusNoError, val)
}

func (c *RawAsciiClient) Increment(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.countRequest("incr", key, delta, initValue, expiration)
}

func (c *RawAsciiClient) Decrement(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.countRequest("decr", key, delta, initValue, expiration)
}

func (c *RawAsciiClient) Flush(expiration uint32) Response {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.writeStrings(
		"flush_all ",
		strconv.FormatUint(uint64(expiration), 10),
		"\r\n")
	if err != nil {
		return NewErrorResponse(err)
	}

	err = c.flushWriter()
	if err != nil {
		return NewErrorResponse(err)
	}

	line, err := c.readLine()
	if err != nil {
		return NewErrorResponse(err)
	}

	_ = c.checkEmptyBuffers()

	if line != "OK" {
		// memcached returned an error message.  This should never happen
		// according to the docs.
		return NewErrorResponse(errors.New(line))
	}

	return NewResponse(StatusNoError)
}

func (c *RawAsciiClient) Stat(statsKey string) StatResponse {
	shardEntries := make(map[int](map[string]string))
	entries := make(map[string]string)
	shardEntries[c.ShardId()] = entries

	if statsKey != "" {
		return NewStatErrorResponse(
			errors.New("Ascii protocol does not support specific stats lookup"),
			shardEntries)
	}

	var err error

	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = c.writeStrings("stats\r\n")
	if err != nil {
		return NewStatErrorResponse(err, shardEntries)
	}

	err = c.flushWriter()
	if err != nil {
		return NewStatErrorResponse(err, shardEntries)
	}

	for {
		line, err := c.readLine()
		if err != nil {
			NewStatErrorResponse(err, shardEntries)
		}

		if line == "END" {
			break
		}

		// line is of the form: STAT <key> <value>
		slice := strings.SplitN(line, " ", 3)

		if len(slice) != 3 || slice[0] != "STAT" {
			// The channel is no longer in valid state since we're exiting
			// stats mid stream.
			c.validState = false
			return NewStatErrorResponse(errors.New(line), shardEntries)
		}

		entries[slice[1]] = slice[2]
	}

	_ = c.checkEmptyBuffers()

	return NewStatResponse(StatusNoError, shardEntries)
}

func (c *RawAsciiClient) Version() VersionResponse {
	versions := make(map[int]string, 1)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.writeStrings("version\r\n")
	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	err = c.flushWriter()
	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	line, err := c.readLine()
	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	_ = c.checkEmptyBuffers()

	if !strings.HasPrefix(line, "VERSION ") {
		// memcached returned an error message.
		return NewVersionErrorResponse(errors.New(line), versions)
	}

	versions[c.ShardId()] = line[len("VERSION "):len(line)]

	return NewVersionResponse(StatusNoError, versions)
}

func (c *RawAsciiClient) Verbosity(verbosity uint32) Response {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.writeStrings(
		"verbosity ",
		strconv.FormatUint(uint64(verbosity), 10),
		"\r\n")
	if err != nil {
		return NewErrorResponse(err)
	}

	err = c.flushWriter()
	if err != nil {
		return NewErrorResponse(err)
	}

	line, err := c.readLine()
	if err != nil {
		return NewErrorResponse(err)
	}

	_ = c.checkEmptyBuffers()

	if line != "OK" {
		// memcached returned an error message.  This should never happen
		// according to the docs.
		return NewErrorResponse(errors.New(line))
	}

	return NewResponse(StatusNoError)
}
