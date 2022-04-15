package bytes2

import (
	"io"
	"io/ioutil"

	. "gopkg.in/check.v1"
)

type MultiChunksReaderSuite struct {
	reader *MultiChunksReader
}

var _ = Suite(&MultiChunksReaderSuite{})

func (s *MultiChunksReaderSuite) SetUpTest(c *C) {
	s.reader = NewMultiChunksReader([][]byte{
		[]byte("012"),
		[]byte("3456"),
		[]byte("78"),
		[]byte("9"),
		[]byte("abcdef"),
		[]byte("ghijklm"),
		[]byte("nopq"),
		[]byte("rstuvwxyz"),
	})
}

func (s *MultiChunksReaderSuite) TestSize(c *C) {
	c.Assert(s.reader.Size(), Equals, int64(36))
}

func (s *MultiChunksReaderSuite) TestBasicRead(c *C) {
	buf := make([]byte, 5)

	n, err := s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(buf), Equals, "01234")

	n, err = s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(buf), Equals, "56789")

	n, err = s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(buf), Equals, "abcde")

	n, err = s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(buf), Equals, "fghij")

	n, err = s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(buf), Equals, "klmno")

	n, err = s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(buf), Equals, "pqrst")

	n, err = s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 5)
	c.Assert(string(buf), Equals, "uvwxy")

	n, err = s.reader.Read(buf)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
	c.Assert(string(buf[:1]), Equals, "z")

	n, err = s.reader.Read(buf)
	c.Assert(err, Equals, io.EOF)
	c.Assert(n, Equals, 0)
}

func (s *MultiChunksReaderSuite) TestSeekFromStart(c *C) {
	pos, err := s.reader.Seek(10, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))

	result, err := ioutil.ReadAll(s.reader)
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, "abcdefghijklmnopqrstuvwxyz")

	pos, err = s.reader.Seek(20, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(20))

	result, err = ioutil.ReadAll(s.reader)
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, "klmnopqrstuvwxyz")

	pos, err = s.reader.Seek(30, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(30))

	result, err = ioutil.ReadAll(s.reader)
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, "uvwxyz")
}

func (s *MultiChunksReaderSuite) TestSeekFromStartPassEOF(c *C) {
	pos, err := s.reader.Seek(s.reader.Size()+10, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, s.reader.Size()+int64(10))

	n, err := s.reader.Read([]byte{0})
	c.Assert(err, Equals, io.EOF)
	c.Assert(n, Equals, 0)
}

func (s *MultiChunksReaderSuite) TestInvalidSeekFromStartNegativePos(c *C) {
	// setup
	pos, err := s.reader.Seek(10, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))

	// actual test
	_, err = s.reader.Seek(-1, io.SeekStart)
	c.Assert(err, NotNil)

	// pos is unmodified
	pos, err = s.reader.Seek(0, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))
}

func (s *MultiChunksReaderSuite) TestSeekFromCurrentNoOp(c *C) {
	buf := make([]byte, 1)
	for i, char := range []byte("0123456789abcdefghijklmnopqrstuvwxyz") {
		pos, err := s.reader.Seek(0, io.SeekCurrent)
		c.Assert(err, IsNil)
		c.Assert(pos, Equals, int64(i))

		n, err := s.reader.Read(buf)
		c.Assert(err, IsNil)
		c.Assert(n, Equals, 1)
		c.Assert(string(buf), Equals, string([]byte{char}))
	}

	n, err := s.reader.Read(buf)
	c.Assert(err, Equals, io.EOF)
	c.Assert(n, Equals, 0)
}

func (s *MultiChunksReaderSuite) TestSeekFromCurrent(c *C) {
	pos, err := s.reader.Seek(15, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(15))

	pos, err = s.reader.Seek(5, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(20))

	result, err := ioutil.ReadAll(s.reader)
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, "klmnopqrstuvwxyz")

	pos, err = s.reader.Seek(15, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(15))

	pos, err = s.reader.Seek(-5, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))

	result, err = ioutil.ReadAll(s.reader)
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, "abcdefghijklmnopqrstuvwxyz")
}

func (s *MultiChunksReaderSuite) TestSeekFromCurrentPassEOF(c *C) {
	pos, err := s.reader.Seek(15, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(15))

	pos, err = s.reader.Seek(s.reader.Size(), io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, s.reader.Size()+int64(15))

	n, err := s.reader.Read([]byte{0})
	c.Assert(err, Equals, io.EOF)
	c.Assert(n, Equals, 0)
}

func (s *MultiChunksReaderSuite) TestInvalidSeekFromCurrentNegativePos(c *C) {
	// setup
	pos, err := s.reader.Seek(10, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))

	// actual test
	_, err = s.reader.Seek(-11, io.SeekCurrent)
	c.Assert(err, NotNil)

	// pos is unmodified
	pos, err = s.reader.Seek(0, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))
}

func (s *MultiChunksReaderSuite) TestSeekFromEnd(c *C) {
	pos, err := s.reader.Seek(-5, io.SeekEnd)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, s.reader.Size()-int64(5))

	result, err := ioutil.ReadAll(s.reader)
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, "vwxyz")

	pos, err = s.reader.Seek(-10, io.SeekEnd)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, s.reader.Size()-int64(10))

	result, err = ioutil.ReadAll(s.reader)
	c.Assert(err, IsNil)
	c.Assert(string(result), Equals, "qrstuvwxyz")
}

func (s *MultiChunksReaderSuite) TestSeekFromEndPassEOF(c *C) {
	pos, err := s.reader.Seek(10, io.SeekEnd)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, s.reader.Size()+int64(10))

	n, err := s.reader.Read([]byte{0})
	c.Assert(err, Equals, io.EOF)
	c.Assert(n, Equals, 0)
}

func (s *MultiChunksReaderSuite) TestInvalidSeekFromEndNegativePos(c *C) {
	// setup
	pos, err := s.reader.Seek(10, io.SeekStart)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))

	// actual test
	_, err = s.reader.Seek(-(s.reader.Size() + 1), io.SeekEnd)
	c.Assert(err, NotNil)

	// pos is unmodified
	pos, err = s.reader.Seek(0, io.SeekCurrent)
	c.Assert(err, IsNil)
	c.Assert(pos, Equals, int64(10))
}
