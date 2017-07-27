package io2

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"

	. "gopkg.in/check.v1"
)

type WriteToConnSuite struct {
	testData string
}

func (wrself *WriteToConnSuite) SetUpTest(c *C) {
	// use real test data (to place nicely with compression, etc)
	// go through all the words in localWords O(n) times, skipping some words each time
	// and reassemble the limited word list into something nearly a megabyte in size
	localWords := []string{}
	for skip := 1; skip < len(words); skip += 6 {
		lskip := skip
		for i := 0; i < len(words); i += (lskip % 16) {
			localWords = append(localWords,
				words[i]+string([]byte{byte(i*len(words)+skip)%32 + byte('a')}))
			if lskip > 1 {
				lskip -= 1
			}
		}
	}
	wrself.testData = strings.Join(localWords, " _ ")

}

// this function simply doubles the length of the input and closes the connection
func asciiToUtf16(c *C, p1 net.Conn) {
	var ibuf [4096]byte
	var obuf [8192]byte
	defer func() { c.Assert(p1.Close(), IsNil) }()
	for {
		count, err := p1.Read(ibuf[:])
		if count > 0 {
			for i := 0; i < count; i += 1 {
				obuf[i*2] = ibuf[i]
				obuf[i*2+1] = 1
			}
		}
		_, werr := p1.Write(obuf[:count*2])
		c.Assert(werr, IsNil)
		if err != nil {
			c.Assert(err, Equals, io.EOF)
			break
		}
	}
}

// this is a more forgiving ascii converter that allows failure in the downstream without panic
func asciiToUtf16CanFail(c *C, p1 net.Conn) {
	var ibuf [4096]byte
	var obuf [8192]byte
	defer func() { c.Assert(p1.Close(), IsNil) }()
	for {
		count, err := p1.Read(ibuf[:])
		if count > 0 {
			for i := 0; i < count; i += 1 {
				obuf[i*2] = ibuf[i]
				obuf[i*2+1] = 1
			}
		}
		_, werr := p1.Write(obuf[:count*2])
		if werr != nil {
			break
		}
		if err != nil {
			c.Assert(err, Equals, io.EOF)
			break
		}
	}

}

// this function closes the upstream early and returns (through the chan) num bytes
// written. It makes up a termination packet full of byte(2) then byte(3)
// to allow errors some time to propagate (i.e. that the short read happened before the write)
func earlyReadClose(c *C, p1 net.Conn, dataWrittenChan chan int) {
	var ibuf [4096]byte
	var obuf [8192]byte
	var late bool
	var closed bool
	var dataWritten int
	for {
		if closed {
			for i := range obuf {
				obuf[i] = 2
			}
			cnt, hurrahErr := p1.Write(obuf[:])
			dataWritten += cnt
			c.Assert(hurrahErr, IsNil)
			for i := range obuf {
				obuf[i] = 3
			}
			cnt, hurrahErr = p1.Write(obuf[:])
			dataWritten += cnt
			c.Assert(hurrahErr, IsNil)
			break
		}
		count, err := p1.Read(ibuf[:])
		if count > 0 {
			for i := 0; i < count; i += 1 {
				obuf[i*2] = ibuf[i]
				obuf[i*2+1] = 1
			}
		}
		cnt, werr := p1.Write(obuf[:count*2])
		dataWritten += cnt
		c.Assert(werr, IsNil)
		if err != nil {
			c.Assert(err, Equals, io.EOF)
			break
		}
		if late {
			p1.(*net.UnixConn).CloseRead()
			closed = true
		}
		late = true
	}
	c.Assert(p1.Close(), IsNil)
	dataWrittenChan <- dataWritten
}

// this function closes the downstream early before the input was entirely consumed
// then it closes the channel
func earlyWriteClose(c *C, p1 net.Conn, dataWrittenChan chan int) {
	var ibuf [4096]byte
	var obuf [8192]byte
	dataWritten := 0
	first := true
	var closed bool
	for {
		count, err := p1.Read(ibuf[:])
		if count > 0 {
			for i := 0; i < count; i += 1 {
				obuf[i*2] = ibuf[i]
				obuf[i*2+1] = 1
			}
		}
		if !closed {
			dataWritten += count * 2
			_, werr := p1.Write(obuf[:count*2])
			c.Assert(werr, IsNil)
		}
		if err != nil {
			c.Assert(err, Equals, io.EOF)
			break
		}
		if !first {
			closed = true
			p1.(*net.UnixConn).CloseRead()
		}
		first = false
	}
	c.Assert(p1.Close(), IsNil)
	dataWrittenChan <- dataWritten
}

// establishes a pair of bidirectional connections connected to one another
// that both have the DuplexShutdown capability
func MakeUnixPipe() (ConnWithDuplexShutdown, ConnWithDuplexShutdown, error) {
	var uuid [16]byte
	rand.Read(uuid[:])
	socketName := "/tmp/" + hex.EncodeToString(uuid[:])
	defer func() { _ = os.RemoveAll(socketName) }()
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketName, Net: "unix"})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = l.Close() }()
	var ret0 ConnWithDuplexShutdown
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ret0, err = l.AcceptUnix()
		wg.Done()
	}()
	ret1, cerr := net.DialUnix("unix", nil, &net.UnixAddr{Name: socketName, Net: "unix"})
	wg.Wait()
	if cerr != nil || err != nil {
		if err == nil {
			err = cerr
		}
		return nil, nil, err
	}
	return ret0, ret1, nil
}

// test that the asciiToUtf16 works properly as a writer in the absence of errors
func (wrself *WriteToConnSuite) TestWriteToPipe(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	go asciiToUtf16(c, p1)
	var utf16Output bytes.Buffer
	wr, err := NewWriteConn(p0, &utf16Output)
	c.Assert(err, IsNil)
	td := []byte(wrself.testData)
	_, err = wr.Write(td)
	c.Assert(err, IsNil)
	err = wr.Close()
	c.Assert(err, IsNil)
	xout := utf16Output.Bytes()
	for index := range td {
		c.Assert(xout[index*2], Equals, td[index])
		c.Assert(xout[index*2+1], Equals, byte(1))
	}
	c.Assert(len(xout), Equals, 2*len(td))
}

// test that the asciiToUtf16 returns an error when the connected process terminates writing early
func (wrself *WriteToConnSuite) TestShortWriteError(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	dataWrittenChan := make(chan int)

	go earlyReadClose(c, p1, dataWrittenChan)
	var utf16Output bytes.Buffer
	wr, err := NewWriteConn(p0, &utf16Output)
	c.Assert(err, IsNil)
	td := []byte(wrself.testData)
	any_ok_writes := false
	any_erroring_writes := false
	for i := 0; i < len(td); i += 127 {
		min127 := i + 127
		if min127 > len(td) {
			min127 = len(td)
		}
		_, err = wr.Write(td[i:min127])
		if err == nil {
			any_ok_writes = true
		} else if err != nil {
			if err != io.EOF {
				any_erroring_writes = true
			}
			break
		}
	}
	if !any_ok_writes {
		c.Assert(err, IsNil)
	}
	if !any_erroring_writes {
		c.Assert(err, Not(IsNil))
	}
	err = wr.Close()
	c.Assert(err, IsNil)
	xout := utf16Output.Bytes()
	twofound := false
	threefound := false
	dataWritten := <-dataWrittenChan
	c.Assert(len(xout), Equals, dataWritten)
	for index := range td {
		if index*2+1 >= dataWritten {
			break
		}
		if xout[index*2] == td[index] {
			c.Assert(xout[index*2], Equals, td[index])
			c.Assert(xout[index*2+1], Equals, byte(1))
		} else if twofound || xout[index*2] == 2 || xout[index*2+1] == 2 {
			if threefound || xout[index*2] == 3 || xout[index*2+1] == 3 {
				c.Assert(xout[index*2], Equals, byte(3))
				c.Assert(xout[index*2+1], Equals, byte(3))
			} else {
				c.Assert(xout[index*2], Equals, byte(2))
				c.Assert(xout[index*2+1], Equals, byte(2))
				twofound = true
			}
		} else {
			c.Assert(xout[index*2], Equals, td[index])
			c.Assert(xout[index*2+1], Equals, byte(1))
		}
	}
	c.Assert(len(xout), Equals, dataWritten)
}

// test that the asciiToUtf16 returns an error when the connected process terminates reads early
func (wrself *WriteToConnSuite) TestShortReadError(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	dataWrittenChan := make(chan int)
	go earlyWriteClose(c, p1, dataWrittenChan)
	var utf16Output bytes.Buffer
	wr, err := NewWriteConn(p0, &utf16Output)
	c.Assert(err, IsNil)
	td := []byte(wrself.testData)
	_, err = wr.Write(td)
	c.Assert(err, Not(IsNil))
	err = wr.Close()
	c.Assert(err, IsNil)
	xout := utf16Output.Bytes()
	dataWritten := <-dataWrittenChan
	for index := range td {
		if dataWritten <= index*2+1 {
			break
		}
		c.Assert(xout[index*2], Equals, td[index])
		c.Assert(xout[index*2+1], Equals, byte(1))
	}
	c.Assert(len(xout), Equals, dataWritten)
}

type ErrOnWrite struct {
	writer bytes.Buffer
	err    error
}

func (eowself *ErrOnWrite) Write(data []byte) (int, error) {
	_, _ = eowself.writer.Write(data)
	return len(data), eowself.err
}
func (eowself *ErrOnWrite) Close() error {
	return nil
}

type ErrOnWriteClose struct {
	writer bytes.Buffer
	err    error
}

func (eocself *ErrOnWriteClose) Write(data []byte) (int, error) {
	return eocself.writer.Write(data)
}
func (eocself *ErrOnWriteClose) Close() error {
	return eocself.err
}

// test that when the downstream writer errors, those errors are passed back to the upstream Writer
func (wrself *WriteToConnSuite) TestErroringWriterPipe(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	go asciiToUtf16CanFail(c, p1)
	xerr := errors.New("ErroringWrite")
	utf16Output := ErrOnWrite{err: xerr}
	wr, err := NewWriteConn(p0, &utf16Output)
	c.Assert(err, IsNil)
	td := []byte(wrself.testData)
	_, err = wr.Write(td)
	hasFoundXerr := err == xerr

	err = wr.Close()
	if hasFoundXerr {
		c.Assert(err, IsNil)
	} else {
		c.Assert(err, Equals, xerr)
	}
	xout := utf16Output.writer.Bytes()
	for index := range td {
		if index*2+1 < len(xout) {
			c.Assert(xout[index*2], Equals, td[index])
			c.Assert(xout[index*2+1], Equals, byte(1))
		}
	}
}

// test that when the downstream writer errors on close, that those errors are passed back
func (wrself *WriteToConnSuite) TestErroringClosePipe(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	go asciiToUtf16(c, p1)
	xerr := errors.New("ErroringWrite")
	utf16Output := ErrOnWriteClose{err: xerr}
	wr, err := NewWriteConn(p0, &utf16Output)
	c.Assert(err, IsNil)
	td := []byte(wrself.testData)
	_, err = wr.Write(td)
	c.Assert(err, IsNil)
	err = wr.Close()
	c.Assert(err, Equals, xerr)
	xout := utf16Output.writer.Bytes()
	for index := range td {
		c.Assert(xout[index*2], Equals, td[index])
		c.Assert(xout[index*2+1], Equals, byte(1))
	}
	c.Assert(len(xout), Equals, 2*len(td))
}

var _ = Suite(&WriteToConnSuite{})
