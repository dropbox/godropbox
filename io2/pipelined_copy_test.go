// +build go1.5

package io2

import (
	"bytes"
	"fmt"
	"io"
	"time"

	. "gopkg.in/check.v1"
)

type fakeReader struct {
	pause time.Duration

	chunks [][]byte
	err    error
}

func (r *fakeReader) Read(buf []byte) (int, error) {
	if r.pause > 0 {
		time.Sleep(r.pause)
	}

	if len(r.chunks) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}

	if len(r.chunks[0]) <= len(buf) {
		chunk := r.chunks[0]
		r.chunks = r.chunks[1:]

		n := copy(buf, chunk)
		if n != len(chunk) {
			panic("BAD COPY")
		}

		return n, nil
	}
	chunk := r.chunks[0][:len(buf)]
	r.chunks[0] = r.chunks[0][len(buf):]

	n := copy(buf, chunk)
	if n != len(buf) {
		panic("BAD COPY")
	}

	return len(buf), nil
}

type fakeSlowReader struct {
	data string
	pos  int
}

func (r *fakeSlowReader) Read(buf []byte) (int, error) {
	if len(r.data) > r.pos {
		buf[0] = byte(r.data[r.pos])
		r.pos++
		return 1, nil
	}

	return 0, io.EOF
}

type fakeWriter struct {
	buf          bytes.Buffer
	errCountDown int
	err          error
}

func (w *fakeWriter) Write(b []byte) (int, error) {
	n, err := w.buf.Write(b)
	if err != nil {
		panic(err)
	}

	w.errCountDown--
	if w.errCountDown < 0 {
		return n, w.err
	}

	return n, nil
}

type PipelinedCopySuite struct {
	reader *fakeReader
	writer *fakeWriter

	copier *circularBufferCopier
}

var _ = Suite(&PipelinedCopySuite{})

func (s *PipelinedCopySuite) SetUpTest(c *C) {
	s.reader = &fakeReader{}
	s.writer = &fakeWriter{}

	s.copier = newCircularBufferCopier(
		s.writer,
		s.reader,
		4,
		10,
		false)

	// sanity check
	c.Assert(s.copier.numBuffers, Equals, 4)
	c.Assert(s.copier.bufferSize, Equals, 10)
	c.Assert(s.copier.minChunkRead, Equals, 9)
}

func (s *PipelinedCopySuite) AddChunks(chunks ...[]byte) {
	s.reader.chunks = append(s.reader.chunks, chunks...)
}

func (s *PipelinedCopySuite) Ready(c *C, val string, isEOF bool) {
	if len(val) > 10 {
		panic("BAD INPUT")
	}

	b := &buffer{
		array: make([]byte, 10, 10),
		size:  len(val),
		eof:   isEOF,
	}
	copy(b.array, []byte(val))

	select {
	case s.copier.readyChan <- b:
		// ok
	default:
		c.Log("failed to put")
		c.FailNow()
	}
}

func (s *PipelinedCopySuite) Recycle(c *C, buf []byte) {
	select {
	case s.copier.recycleChan <- buf:
		// ok
	default:
		c.Log("failed to recycle")
		c.FailNow()
	}
}

func (s *PipelinedCopySuite) TestMinRead(c *C) {
	// 90% of buffer size
	c.Assert(minRead(1), Equals, 1)
	c.Assert(minRead(2), Equals, 2)
	c.Assert(minRead(10), Equals, 9)
	c.Assert(minRead(100), Equals, 90)
	c.Assert(minRead(1000), Equals, 900)
	c.Assert(minRead(10000), Equals, 9000)

	// buffer size - 1024
	c.Assert(minRead(14000), Equals, 12976)
	c.Assert(minRead(15*1024), Equals, 14*1024)
	c.Assert(minRead(16*1024), Equals, 15*1024)
}

func (s *PipelinedCopySuite) AssertReady(
	c *C,
	expected string,
	expectedBuf []byte,
	isEOF bool) {

	select {
	case b := <-s.copier.readyChan:
		c.Assert(b, NotNil)
		c.Assert(len(b.array), Equals, 10)
		c.Assert(b.size, Equals, len(expected))
		c.Assert(string(b.array[:len(expected)]), Equals, expected)

		c.Assert(b.eof, Equals, isEOF)

		if expectedBuf != nil {
			// pointer check
			c.Assert(string(expectedBuf[:len(expected)]), Equals, expected)
		}
	case <-time.After(time.Second):
		c.Log("no ready buffer")
		c.FailNow()
	}
}

func (s *PipelinedCopySuite) AssertNotReady(c *C) {
	select {
	case <-s.copier.readyChan:
		c.Log("should not return buffer")
		c.FailNow()
	case <-time.After(100 * time.Millisecond):
		// ok
	}
}

func (s *PipelinedCopySuite) AssertRecycle(c *C) {
	select {
	case b := <-s.copier.recycleChan:
		c.Assert(b, NotNil)
		c.Assert(len(b), Equals, 10)
	case <-time.After(time.Second):
		c.Log("no recycle buffer")
		c.FailNow()
	}
}

func (s *PipelinedCopySuite) AssertNotRecycle(c *C) {
	select {
	case <-s.copier.recycleChan:
		c.Log("should not recycle buffer")
		c.FailNow()
	case <-time.After(100 * time.Millisecond):
		// ok
	}
}

func (s *PipelinedCopySuite) AssertErrChan(c *C, expectNil bool) {
	select {
	case err := <-s.copier.errChan:
		if expectNil {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
		}
	case <-time.After(time.Second):
		c.Log("nothing in err chan")
		c.FailNow()
	}
}

func (s *PipelinedCopySuite) TestBasicRead(c *C) {
	s.AddChunks(
		// chunks:
		// "123456789"
		// "01234abcde"
		// "fghijklmno"
		// "pqrstuvwx"
		// "zzfoobarhe"
		// "llo world"
		// "asdfghjkl!"
		[]byte("123456"),
		[]byte("789"),
		[]byte("0"),
		[]byte("1234"),
		[]byte("abcdefghi"),
		[]byte("jklmno"),
		[]byte("pqrstuv"),
		[]byte("w"),
		[]byte("x"),
		[]byte("zz"),
		[]byte("foobar"),
		[]byte("hello world"),
		[]byte("asdfghjkl!"))

	go s.copier.readLoop()

	s.AssertReady(c, "123456789", nil, false)
	s.AssertReady(c, "01234abcde", nil, false)
	s.AssertReady(c, "fghijklmno", nil, false)
	s.AssertReady(c, "pqrstuvwx", nil, false)

	// ran out of buffers
	s.AssertNotReady(c)

	buf1 := make([]byte, 10, 10)
	s.Recycle(c, buf1)

	s.AssertReady(c, "zzfoobarhe", buf1, false)

	// ran out of buffers again
	s.AssertNotReady(c)

	buf2 := make([]byte, 10, 10)
	buf3 := make([]byte, 10, 10)
	s.Recycle(c, buf1)
	s.Recycle(c, buf2)
	s.Recycle(c, buf3)

	s.AssertReady(c, "llo world", buf1, false)
	s.AssertReady(c, "asdfghjkl!", buf2, false)
	s.AssertReady(c, "", buf3, true) // io.EOF

	s.AssertErrChan(c, true)

	c.Assert(s.copier.numCreated, Equals, 4)
}

func (s *PipelinedCopySuite) TestErrUnexpectedEOF(c *C) {
	s.AddChunks(
		// chunks:
		// "01234abcde"
		// "fghijklmno"
		// "pqrstuvwx"
		// "zzfoo"
		[]byte("0"),
		[]byte("1234"),
		[]byte("abcdefghi"),
		[]byte("jklmno"),
		[]byte("pqrstuv"),
		[]byte("w"),
		[]byte("x"),
		[]byte("zz"),
		[]byte("foo"))

	// Add a few items before starting the read loop to test for recycling
	buf1 := make([]byte, 10, 10)
	buf2 := make([]byte, 10, 10)
	buf3 := make([]byte, 10, 10)
	s.Recycle(c, buf1)
	s.Recycle(c, buf2)
	s.Recycle(c, buf3)

	go s.copier.readLoop()

	s.AssertReady(c, "01234abcde", buf1, false)
	s.AssertReady(c, "fghijklmno", buf2, false)
	s.AssertReady(c, "pqrstuvwx", buf3, false)
	s.AssertReady(c, "zzfoo", nil, true) // io.ErrUnexpectedEOF

	s.AssertErrChan(c, true)

	c.Assert(s.copier.numCreated, Equals, 1)
}

func (s *PipelinedCopySuite) TestReadError(c *C) {
	s.AddChunks(
		[]byte("all good."),
		[]byte("o shi"))
	s.reader.err = fmt.Errorf("boom")

	go s.copier.readLoop()

	s.AssertReady(c, "all good.", nil, false)

	s.AssertNotReady(c)

	s.AssertErrChan(c, false)
}

func (s *PipelinedCopySuite) TestReadEarlyExit(c *C) {
	s.AddChunks(
		// chunks:
		// "123456789"
		// "01234abcde"
		// "fghijklmno"
		// "pqrstuvwx"
		// "zzfoobarhe"
		// "llo world"
		// "asdfghjkl!"
		[]byte("123456"),
		[]byte("789"),
		[]byte("0"),
		[]byte("1234"),
		[]byte("abcdefghi"),
		[]byte("jklmno"),
		[]byte("pqrstuv"),
		[]byte("w"),
		[]byte("x"),
		[]byte("zz"),
		[]byte("foobar"),
		[]byte("hello world"),
		[]byte("asdfghjkl!"))

	go s.copier.readLoop()

	s.AssertReady(c, "123456789", nil, false)
	s.AssertReady(c, "01234abcde", nil, false)
	s.AssertReady(c, "fghijklmno", nil, false)
	s.AssertReady(c, "pqrstuvwx", nil, false)

	close(s.copier.earlyExitChan)

	// ran out of buffers
	s.AssertNotReady(c)

	s.AssertErrChan(c, true)
}

func (s *PipelinedCopySuite) TestBasicWrite(c *C) {
	go s.copier.writeLoop()

	s.Ready(c, "123456789", false)
	s.Ready(c, "abcdefghij", false)
	s.Ready(c, "decafbad", true)

	// Recycled the first two buffers. last one is dropped due to EOF.
	s.AssertRecycle(c)
	s.AssertRecycle(c)
	s.AssertNotRecycle(c)

	s.AssertErrChan(c, true)

	expected := "123456789abcdefghijdecafbad"
	c.Assert(s.copier.numWritten, Equals, int64(len(expected)))
	c.Assert(s.writer.buf.String(), Equals, expected)
}

func (s *PipelinedCopySuite) TestWriteError(c *C) {
	s.writer.errCountDown = 2
	s.writer.err = fmt.Errorf("boom")

	s.Ready(c, "foo", false)
	s.Ready(c, "bar", false)
	s.Ready(c, "bad", false)
	s.Ready(c, "asdf", true)

	go s.copier.writeLoop()

	// Recycled the first two buffers. Gave up on the third chunk.
	s.AssertRecycle(c)
	s.AssertRecycle(c)
	s.AssertNotRecycle(c)

	// Write loop didn't pick up the last chunk
	s.AssertReady(c, "asdf", nil, true)

	s.AssertErrChan(c, false)

	expected := "foobarbad"
	c.Assert(s.copier.numWritten, Equals, int64(len(expected)))
	c.Assert(s.writer.buf.String(), Equals, expected)
}

func (s *PipelinedCopySuite) TestWriteEarlyExit(c *C) {
	go s.copier.writeLoop()

	s.Ready(c, "foo", false)

	s.AssertRecycle(c)

	close(s.copier.earlyExitChan)

	s.AssertErrChan(c, true)

	c.Assert(s.writer.buf.String(), Equals, "foo")
}

func (s *PipelinedCopySuite) TestE2ECopy(c *C) {
	s.AddChunks(
		// chunks:
		// "123456789"
		// "01234abcde"
		// "fghijklmno"
		// "pqrstuvwx"
		// "zzfoobarhe"
		// "llo world"
		// "asdfghjkl!"
		[]byte("123456"),
		[]byte("789"),
		[]byte("0"),
		[]byte("1234"),
		[]byte("abcdefghi"),
		[]byte("jklmno"),
		[]byte("pqrstuv"),
		[]byte("w"),
		[]byte("x"),
		[]byte("zz"),
		[]byte("foobar"),
		[]byte("hello world"),
		[]byte("asdfghjkl!"))

	var n int64
	var err error
	done := make(chan struct{})
	go func() {
		n, err = s.copier.execute()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(time.Second):
		c.Log("copier didn't finish")
		c.FailNow()
	}

	c.Assert(err, IsNil)

	expected := ("12345678901234abcdefghijklmnopqrstuvwx" +
		"zzfoobarhello worldasdfghjkl!")
	c.Assert(n, Equals, int64(len(expected)))
	c.Assert(s.writer.buf.String(), Equals, expected)
}

func (s *PipelinedCopySuite) TestE2EReadError(c *C) {
	s.AddChunks(
		[]byte("all good. "),
		[]byte("still ok."),
		[]byte("o shi"))
	s.reader.err = fmt.Errorf("boom")
	s.reader.pause = 10 * time.Millisecond

	var n int64
	var err error
	done := make(chan struct{})
	go func() {
		n, err = s.copier.execute()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(time.Second):
		c.Log("copier didn't finish")
		c.FailNow()
	}

	c.Assert(err, NotNil)

	expected := "all good. still ok."
	c.Assert(n, Equals, int64(len(expected)))
	c.Assert(s.writer.buf.String(), Equals, expected)
}

func (s *PipelinedCopySuite) TestE2EWriteError(c *C) {
	s.AddChunks(
		[]byte("0123456789"),
		[]byte("abcdefghij"),
		[]byte("sad panda"),
		[]byte("lalala"))

	s.writer.errCountDown = 2
	s.writer.err = fmt.Errorf("boom")

	var n int64
	var err error
	done := make(chan struct{})
	go func() {
		n, err = s.copier.execute()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(time.Second):
		c.Log("copier didn't finish")
		c.FailNow()
	}

	c.Assert(err, NotNil)

	expected := "0123456789abcdefghijsad panda"
	c.Assert(n, Equals, int64(len(expected)))
	c.Assert(s.writer.buf.String(), Equals, expected)
}

func (s *PipelinedCopySuite) TestFastStart(c *C) {
	reader := &fakeSlowReader{
		data: "01234567890abcdefghijklmnopqrstuvwxyz",
		pos:  0,
	}

	s.copier = newCircularBufferCopier(
		s.writer,
		reader,
		4,
		10,
		true)

	go s.copier.readLoop()

	s.AssertReady(c, "0", nil, false)
	s.AssertReady(c, "12", nil, false)
	s.AssertReady(c, "3456", nil, false)
	s.AssertReady(c, "7890abcd", nil, false)

	// ran out of buffers
	s.AssertNotReady(c)

	s.Recycle(c, make([]byte, 10, 10))
	s.Recycle(c, make([]byte, 10, 10))
	s.Recycle(c, make([]byte, 10, 10))
	s.Recycle(c, make([]byte, 10, 10))

	s.AssertReady(c, "efghijklm", nil, false)
	s.AssertReady(c, "nopqrstuv", nil, false)
	s.AssertReady(c, "wxyz", nil, true)
}
