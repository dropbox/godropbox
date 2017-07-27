package memcache

import (
	"bytes"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

// Constant values used in testing the client.
const (
	testCas    = 0xdecafbad
	testKey    = "Hello"
	testFlags  = uint32(0xdeadbeef)
	testExpiry = uint32(0xe10)
)

var testValue = []byte("World")

// A mock ReadWriter with separate read and write buffers.
// Used to test raw client methods that both read from and write to the channel.
type mockReadWriter struct {
	// The buffer that will be used for Read.
	recvBuf *bytes.Buffer
	// The buffer that will be used for Write.
	sendBuf *bytes.Buffer
	// Notification channel for recvBuf.
	receivedChan chan struct{}
	// Notification channel for sendBuf.
	sentChan chan struct{}
}

func newMockReadWriter() *mockReadWriter {
	return &mockReadWriter{
		recvBuf:      &bytes.Buffer{},
		sendBuf:      &bytes.Buffer{},
		receivedChan: make(chan struct{}, 1000),
		sentChan:     make(chan struct{}, 1000),
	}
}

// Implements the Reader interface.
func (rw *mockReadWriter) Read(p []byte) (n int, err error) {
	defer func() { rw.receivedChan <- struct{}{} }()
	return rw.recvBuf.Read(p)
}

// Implements the Writer interface.
func (rw *mockReadWriter) Write(p []byte) (n int, err error) {
	defer func() { rw.sentChan <- struct{}{} }()
	return rw.sendBuf.Write(p)
}

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type RawBinaryClientSuite struct {
	rw     *mockReadWriter
	client *RawBinaryClient
}

var _ = Suite(&RawBinaryClientSuite{})

func (s *RawBinaryClientSuite) SetUpTest(c *C) {
	s.rw = newMockReadWriter()
	s.client = NewRawBinaryClient(0, s.rw).(*RawBinaryClient)
}

func (s *RawBinaryClientSuite) verifyRequestMessage(c *C, code opCode) {
	/*
	     Byte/     0       |       1       |       2       |       3       |
	        /              |               |               |               |
	       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
	       +---------------+---------------+---------------+---------------+
	      0| 0x80          | 0x02          | 0x00          | 0x05          |
	       +---------------+---------------+---------------+---------------+
	      4| 0x08          | 0x00          | 0x00          | 0x00          |
	       +---------------+---------------+---------------+---------------+
	      8| 0x00          | 0x00          | 0x00          | 0x12          |
	       +---------------+---------------+---------------+---------------+
	     12| 0x00          | 0x00          | 0x00          | 0x00          |
	       +---------------+---------------+---------------+---------------+
	     16| 0x00          | 0x00          | 0x00          | 0x00          |
	       +---------------+---------------+---------------+---------------+
	     20| 0xde          | 0xca          | 0xfb          | 0xad          |
	       +---------------+---------------+---------------+---------------+
	     24| 0xde          | 0xad          | 0xbe          | 0xef          |
	       +---------------+---------------+---------------+---------------+
	     28| 0x00          | 0x00          | 0x0e          | 0x10          |
	       +---------------+---------------+---------------+---------------+
	     32| 0x48 ('H')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
	       +---------------+---------------+---------------+---------------+
	     36| 0x6f ('o')    | 0x57 ('W')    | 0x6f ('o')    | 0x72 ('r')    |
	       +---------------+---------------+---------------+---------------+
	     40| 0x6c ('l')    | 0x64 ('d')    |
	       +---------------+---------------+

	      Total 42 bytes (24 byte header, 8 byte extras, 5 byte key and
	                       5 byte value)


	   Field        (offset) (value)
	    Magic        (0)    : reqMagicByte
	    Opcode       (1)    : 0x0X
	    Key length   (2,3)  : 0x0005
	    Extra length (4)    : 0x08
	    Data type    (5)    : 0x00
	    VBucket      (6,7)  : 0x0000
	    Total body   (8-11) : 0x00000012
	    Opaque       (12-15): 0x00000000
	    CAS          (16-23): 0x00000000decafbad
	    Extras              :
	      Flags      (24-27): 0xdeadbeef
	      Expiry     (28-31): 0x00000e10
	    Key          (32-36): The textual string "Hello"
	    Value        (37-41): The textual string "World"
	*/
	var serializedRequestMessage = []byte{
		reqMagicByte, // magic
		uint8(code),  // op code
		0x00, 0x05,   // key length
		0x08,       // extra length
		0x00,       // data type
		0x00, 0x00, // v bucket id
		0x00, 0x00, 0x00, 0x12, // total body length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0xde, 0xca, 0xfb, 0xad, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		0x00, 0x00, 0x0e, 0x10, // expiry
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	c.Assert(s.rw.sendBuf.Bytes(), DeepEquals, serializedRequestMessage)
}

func (s *RawBinaryClientSuite) TestSendRequest(c *C) {
	err := s.client.sendRequest(
		opAdd,
		testCas,         // CAS
		[]byte(testKey), // key
		testValue,       // value
		testFlags,       // flags
		testExpiry)      // expiry

	c.Assert(err, IsNil)
	s.verifyRequestMessage(c, opAdd)
}

func (s *RawBinaryClientSuite) TestSendRequestKeyTooLong(c *C) {
	var key [256]byte
	err := s.client.sendRequest(
		opAdd,
		testCas,
		key[:],
		testValue,
		testFlags,
		testExpiry)
	c.Assert(err, NotNil)
}

func (s *RawBinaryClientSuite) TestRecvResponse(c *C) {
	/*
	    Byte/     0       |       1       |       2       |       3       |
	        /              |               |               |               |
	       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
	       +---------------+---------------+---------------+---------------+
	      0| 0x81          | 0x09          | 0x00          | 0x05          |
	       +---------------+---------------+---------------+---------------+
	      4| 0x04          | 0x00          | 0x00          | 0x02          |
	       +---------------+---------------+---------------+---------------+
	      8| 0x00          | 0x00          | 0x00          | 0x0e          |
	       +---------------+---------------+---------------+---------------+
	     12| 0x00          | 0x00          | 0x00          | 0x00          |
	       +---------------+---------------+---------------+---------------+
	     16| 0x00          | 0x00          | 0x00          | 0x00          |
	       +---------------+---------------+---------------+---------------+
	     20| 0x00          | 0x00          | 0x00          | 0x01          |
	       +---------------+---------------+---------------+---------------+
	     24| 0xde          | 0xad          | 0xbe          | 0xef          |
	       +---------------+---------------+---------------+---------------+
	     28| 0x48 ('H')    | 0x65 ('e')    | 0x6c ('l')    | 0x6c ('l')    |
	       +---------------+---------------+---------------+---------------+
	     32| 0x6f ('o')    | 0x57 ('W')    | 0x6f ('o')    | 0x72 ('r')    |
	       +---------------+---------------+---------------+---------------+
	     36| 0x6c ('l')    | 0x64 ('d')    |
	       +---------------+---------------+


	      Total 38 bytes (24 byte header, 4 byte extras, 5 byte key
	                       and 5 byte value)


	   Field        (offset) (value)
	    Magic        (0)    : respMagicByte
	    Opcode       (1)    : 0x09
	    Key length   (2,3)  : 0x0005
	    Extra length (4)    : 0x04
	    Data type    (5)    : 0x00
	    Status       (6,7)  : 0x0002
	    Total body   (8-11) : 0x0000000e
	    Opaque       (12-15): 0x00000000
	    CAS          (16-23): 0x0000000000000001
	    Extras              :
	      Flags      (24-27): 0xdeadbeef
	    Key          (28-32): The textual string: "Hello"
	    Value        (33-37): The textual string: "World"
	*/
	var serializedResponseMessage = []byte{
		respMagicByte, // magic
		uint8(opGetQ), // op code
		0x00, 0x05,    // key length
		0x04,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x0e, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	status, cas, key, val, err := s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, IsNil)

	c.Assert(status, Equals, StatusKeyExists)
	c.Assert(cas, Equals, uint64(1))
	c.Assert(key, DeepEquals, []byte("Hello"))
	c.Assert(val, DeepEquals, []byte("World"))
	c.Assert(flags, Equals, uint32(0xdeadbeef))
}

func (s *RawBinaryClientSuite) TestRecvResponseNoExtrasByteWithExtrasArg(c *C) {

	var serializedResponseMessage = []byte{
		respMagicByte, // magic
		0x09,          // op code
		0x00, 0x05,    // key length
		0x00,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x0e, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawBinaryClientSuite) TestRecvResponseNotEnoughExtrasBytes(c *C) {
	var serializedResponseMessage = []byte{
		respMagicByte, // magic
		0x09,          // op code
		0x00, 0x05,    // key length
		0x04,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x0e, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32
	var otherExtra uint32 // unexpected extra field

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags,
		&otherExtra)
	c.Assert(err, NotNil)
}

func (s *RawBinaryClientSuite) TestRecvResponseTooManExtrasBytes(c *C) {
	var serializedResponseMessage = []byte{
		respMagicByte, // magic
		0x09,          // op code
		0x00, 0x05,    // key length
		0x04,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x0e, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint16 // normally should be 32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawBinaryClientSuite) TestRecvResponseBadTotalLength(c *C) {
	var serializedResponseMessage = []byte{
		respMagicByte, // magic
		0x09,          // op code
		0x00, 0x05,    // key length
		0x04,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x01, // (bad) total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawBinaryClientSuite) TestRecvResponseBadMagic(c *C) {
	var serializedResponseMessage = []byte{
		0x82,       // (bad) magic
		0x09,       // op code
		0x00, 0x05, // key length
		0x04,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x0e, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawBinaryClientSuite) TestRecvResponseBadOpCode(c *C) {

	var serializedResponseMessage = []byte{
		respMagicByte, // magic
		0x03,          // (bad) op code
		0x00, 0x05,    // key length
		0x04,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x0e, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func createTestItem() *Item {
	return &Item{
		Key:           testKey,
		Value:         testValue,
		Flags:         testFlags,
		Expiration:    testExpiry,
		DataVersionId: testCas,
	}
}

func (s *RawBinaryClientSuite) performMutateRequestTest(
	c *C,
	code opCode,
	isMulti bool) {

	// Populate the add response.
	var serializedResponseMessage = []byte{
		respMagicByte, // magic
		uint8(code),   // op code
		0x00, 0x04,    // key length
		0x0,        // extras length
		0x0,        // data type
		0x00, 0x00, // status
		0x00, 0x00, 0x00, 0x04, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		'H', 'e', 'l', 'l', 'o', // key
	}
	_, err := s.rw.recvBuf.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	// Kick off the add.
	item := createTestItem()
	respChan := make(chan []MutateResponse)
	go func() {
		if code == opAdd {
			if isMulti {
				respChan <- s.client.AddMulti([]*Item{item})
			} else {
				respChan <- []MutateResponse{s.client.Add(item)}
			}
		} else if code == opSet {
			if isMulti {
				respChan <- s.client.SetMulti([]*Item{item})
			} else {
				respChan <- []MutateResponse{s.client.Set(item)}
			}
		}
	}()

	// Wait for the request buffer to be populated.
	select {
	case <-s.rw.sentChan:
	case <-time.After(500 * time.Millisecond):
		c.Fatal("Timed out waiting for client to send request")
	}

	s.verifyRequestMessage(c, code)

	resps := <-respChan
	c.Assert(resps, HasLen, 1)
	resp := resps[0]
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Key(), Equals, testKey)
	c.Assert(resp.DataVersionId(), Equals, uint64(1))
}

func (s *RawBinaryClientSuite) TestAddRequest(c *C) {
	s.performMutateRequestTest(c, opAdd, false)
}

func (s *RawBinaryClientSuite) TestAddMultiRequest(c *C) {
	s.performMutateRequestTest(c, opAdd, true)
}

func (s *RawBinaryClientSuite) TestSetRequest(c *C) {
	s.performMutateRequestTest(c, opSet, false)
}

func (s *RawBinaryClientSuite) TestSetMultiRequest(c *C) {
	s.performMutateRequestTest(c, opSet, true)
}

func (s *RawBinaryClientSuite) TestGetMultiDupKeys(c *C) {
	expectedFooReq := []byte{
		reqMagicByte, // magic
		uint8(opGet), // op code
		0x00, 0x03,   // key length
		0x00,       // extra length
		0x00,       // data type
		0x00, 0x00, // v bucket id
		0x00, 0x00, 0x00, 0x3, // total body length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, // flags
		0x00, 0x00, 0x00, 0x00, // expiry
		'f', 'o', 'o', // key
	}

	fooResp := []byte{
		respMagicByte, // magic
		uint8(opGet),  // op code
		0x00, 0x03,    // key length
		0x04,       // extras length
		0x0,        // data type
		0x00, 0x00, // status
		0x00, 0x00, 0x00, 0x0a, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		0xde, 0xad, 0xbe, 0xef, // flags
		'f', 'o', 'o', // key
		'F', 'O', 'O', // value
	}

	s.rw.recvBuf.Write(fooResp)

	results := s.client.GetMulti([]string{"foo", "foo"})

	c.Assert(s.rw.sendBuf.Bytes(), DeepEquals, expectedFooReq)

	c.Assert(results, HasKey, "foo")
	c.Assert(string(results["foo"].Value()), Equals, "FOO")
}
