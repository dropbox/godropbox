package memcache

import (
	"bytes"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type RawClientSuite struct {
	channel *bytes.Buffer
	client  *RawClient
}

var _ = Suite(&RawClientSuite{})

func (s *RawClientSuite) SetUpTest(c *C) {
	s.channel = &bytes.Buffer{}
	s.client = NewRawClient(0, s.channel).(*RawClient)
}

func (s *RawClientSuite) TestSendRequest(c *C) {
	err := s.client.sendRequest(
		opAdd,
		0xdecafbad,         // CAS
		[]byte("Hello"),    // key
		[]byte("World"),    // value
		uint32(0xdeadbeef), // flags
		uint32(0xe10))      // expiry
	c.Assert(err, IsNil)

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
	    Magic        (0)    : 0x80
	    Opcode       (1)    : 0x02
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
		0x80,       // magic
		0x02,       // op code
		0x00, 0x05, // key length
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

	c.Assert(s.channel.Bytes(), DeepEquals, serializedRequestMessage)
}

func (s *RawClientSuite) TestRecvResponse(c *C) {
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
	    Magic        (0)    : 0x81
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
		0x81,       // magic
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

	_, err := s.channel.Write(serializedResponseMessage)
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

func (s *RawClientSuite) TestRecvResponseNoExtrasByteWithExtrasArg(c *C) {

	var serializedResponseMessage = []byte{
		0x81,       // magic
		0x09,       // op code
		0x00, 0x05, // key length
		0x00,       // extras length
		0x0,        // data type
		0x00, 0x02, // status
		0x00, 0x00, 0x00, 0x0e, // total length
		0x00, 0x00, 0x00, 0x00, // opaque
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // cas
		'H', 'e', 'l', 'l', 'o', // key
		'W', 'o', 'r', 'l', 'd', // value
	}

	_, err := s.channel.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawClientSuite) TestRecvResponseNotEnoughExtrasBytes(c *C) {
	var serializedResponseMessage = []byte{
		0x81,       // magic
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

	_, err := s.channel.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32
	var otherExtra uint32 // unexpected extra field

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags,
		&otherExtra)
	c.Assert(err, NotNil)
}

func (s *RawClientSuite) TestRecvResponseTooManExtrasBytes(c *C) {
	var serializedResponseMessage = []byte{
		0x81,       // magic
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

	_, err := s.channel.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint16 // normally should be 32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawClientSuite) TestRecvResponseBadTotalLength(c *C) {
	var serializedResponseMessage = []byte{
		0x81,       // magic
		0x09,       // op code
		0x00, 0x05, // key length
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

	_, err := s.channel.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawClientSuite) TestRecvResponseBadMagic(c *C) {
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

	_, err := s.channel.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}

func (s *RawClientSuite) TestRecvResponseBadOpCode(c *C) {

	var serializedResponseMessage = []byte{
		0x81,       // magic
		0x03,       // (bad) op code
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

	_, err := s.channel.Write(serializedResponseMessage)
	c.Assert(err, IsNil)

	var flags uint32

	_, _, _, _, err = s.client.receiveResponse(
		opGetQ,
		&flags)
	c.Assert(err, NotNil)
}
