package bufio2

import (
	"bytes"
	"io"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the go test runner.
func Test(t *testing.T) {
	TestingT(t)
}

type LookAheadBufferSuite struct {
	reader *bytes.Reader
	buffer *LookAheadBuffer
}

var _ = Suite(&LookAheadBufferSuite{})

var testBytes = []byte(
	"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

const bufSize = 5

func (s *LookAheadBufferSuite) SetUpTest(c *C) {
	s.reader = bytes.NewReader(testBytes)
	s.buffer = NewLookAheadBuffer(s.reader, bufSize)
}

func (s *LookAheadBufferSuite) TestPeek(c *C) {
	bytes, err := s.buffer.Peek(0)
	c.Check(bytes, DeepEquals, []byte{})
	c.Check(err, IsNil)
	c.Check(s.buffer.BytesBuffered(), Equals, 0)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte{})
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("\000\000\000\000\000"))

	for i := 1; i <= bufSize; i++ {
		c.Log(i)
		bytes, err := s.buffer.Peek(i)
		c.Check(bytes, DeepEquals, testBytes[:i])
		c.Check(err, IsNil)
		c.Check(s.buffer.BytesBuffered(), Equals, bufSize)
		c.Check(s.buffer.Buffer(), DeepEquals, []byte("abcde"))
		c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("abcde"))
	}
	c.Check(s.reader.Len(), Equals, len(testBytes)-bufSize)

	for i := 1; i < bufSize; i++ {
		bytes, err := s.buffer.Peek(i)
		c.Check(bytes, DeepEquals, testBytes[:i])
		c.Check(err, IsNil)
		c.Check(s.buffer.BytesBuffered(), Equals, bufSize)
		c.Check(s.buffer.Buffer(), DeepEquals, []byte("abcde"))
		c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("abcde"))
	}
	c.Check(s.reader.Len(), Equals, len(testBytes)-bufSize)
}

func (s *LookAheadBufferSuite) TestLookAheadBufferFull(c *C) {
	bytes, err := s.buffer.Peek(bufSize + 1)
	c.Check(bytes, IsNil)
	c.Check(err, Not(IsNil))
}

func (s *LookAheadBufferSuite) TestLookAheadEOFRead(c *C) {
	reader := bytes.NewReader([]byte("abcd"))
	buffer := NewLookAheadBuffer(reader, 6)

	// Consume all bytes in reader
	bytes, err := buffer.Peek(2)
	c.Check(bytes, DeepEquals, []byte("ab"))
	c.Check(err, IsNil)
	c.Check(buffer.BytesBuffered(), Equals, 4)
	c.Check(buffer.Buffer(), DeepEquals, []byte("abcd"))
	c.Check(buffer.RawBuffer(), DeepEquals, []byte("abcd\000\000"))

	bytes, err = buffer.Peek(6)
	c.Check(bytes, DeepEquals, []byte("abcd"))
	c.Check(err, Equals, io.EOF)
}

func (s *LookAheadBufferSuite) TestPeekAll(c *C) {
	bytes, err := s.buffer.PeekAll()
	c.Check(bytes, DeepEquals, []byte("abcde"))
	c.Check(err, IsNil)
	c.Check(s.buffer.BytesBuffered(), Equals, bufSize)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("abcde"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("abcde"))
	c.Check(s.reader.Len(), Equals, len(testBytes)-bufSize)
}

func (s *LookAheadBufferSuite) TestConsume(c *C) {
	s.buffer.PeekAll()
	c.Check(s.buffer.BytesBuffered(), Equals, 5)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("abcde"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("abcde"))

	err := s.buffer.Consume(2)
	c.Check(err, IsNil)
	c.Check(s.buffer.BytesBuffered(), Equals, 3)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("cde"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("cdede"))

	err = s.buffer.Consume(0)
	c.Check(err, IsNil)
	c.Check(s.buffer.BytesBuffered(), Equals, 3)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("cde"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("cdede"))

	err = s.buffer.Consume(1)
	c.Check(err, IsNil)
	c.Check(s.buffer.BytesBuffered(), Equals, 2)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("de"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("deede"))
}

func (s *LookAheadBufferSuite) TestConsumeThenEOF(c *C) {
	reader := bytes.NewReader([]byte("abcdefg"))
	buffer := NewLookAheadBuffer(reader, 5)

	buffer.PeekAll()
	c.Check(buffer.BytesBuffered(), Equals, 5)
	c.Check(buffer.Buffer(), DeepEquals, []byte("abcde"))
	c.Check(buffer.RawBuffer(), DeepEquals, []byte("abcde"))

	err := buffer.Consume(3)
	c.Check(err, IsNil)
	c.Check(buffer.BytesBuffered(), Equals, 2)
	c.Check(buffer.Buffer(), DeepEquals, []byte("de"))
	c.Check(buffer.RawBuffer(), DeepEquals, []byte("decde"))

	bytes, err := buffer.PeekAll()
	c.Check(bytes, DeepEquals, []byte("defg"))
	c.Check(err, Equals, io.EOF)
	c.Check(buffer.BytesBuffered(), Equals, 4)
	c.Check(buffer.Buffer(), DeepEquals, []byte("defg"))
	c.Check(buffer.RawBuffer(), DeepEquals, []byte("defge"))
}

func (s *LookAheadBufferSuite) TestConsumeShortBuffer(c *C) {
	s.buffer.PeekAll()
	c.Check(s.buffer.BytesBuffered(), Equals, bufSize)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("abcde"))

	// ok
	err := s.buffer.Consume(2)
	c.Check(s.buffer.BytesBuffered(), Equals, 3)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("cde"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("cdede"))

	err = s.buffer.Consume(4)
	c.Check(err, Not(IsNil))

	// buffer is not modified after error
	c.Check(s.buffer.BytesBuffered(), Equals, 3)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("cde"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("cdede"))
}

func (s *LookAheadBufferSuite) TestConsumeAll(c *C) {
	s.buffer.Peek(4)
	c.Check(s.buffer.BytesBuffered(), Equals, bufSize)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("abcde"))

	s.buffer.ConsumeAll()
	c.Check(s.buffer.BytesBuffered(), Equals, 0)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte{})
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("abcde"))

	s.buffer.Peek(4)
	c.Check(s.buffer.BytesBuffered(), Equals, bufSize)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("fghij"))

	err := s.buffer.Consume(2)
	c.Check(err, IsNil)
	c.Check(s.buffer.BytesBuffered(), Equals, 3)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte("hij"))
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("hijij"))

	s.buffer.ConsumeAll()
	c.Check(s.buffer.BytesBuffered(), Equals, 0)
	c.Check(s.buffer.Buffer(), DeepEquals, []byte{})
	c.Check(s.buffer.RawBuffer(), DeepEquals, []byte("hijij"))
}
