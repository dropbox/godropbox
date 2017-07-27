package io2

import (
	"bytes"
	"errors"
	. "gopkg.in/check.v1"
	"io"
	"io/ioutil"
	"strings"
)

type ReadFromConnSuite struct {
	testData string
}

func (rcself *ReadFromConnSuite) SetUpTest(c *C) {
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
	rcself.testData = strings.Join(localWords, " _ ")

}

// test that the asciiToUtf16 processor can be treated as a universal io.Reader
func (rcself *ReadFromConnSuite) TestReadFromPipe(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	go asciiToUtf16(c, p1)
	td := []byte(rcself.testData)
	rd, err := NewReadConn(p0, bytes.NewBuffer(td))
	c.Assert(err, IsNil)
	var utf16 []byte
	utf16, err = ioutil.ReadAll(rd)
	c.Assert(err, IsNil)
	err = rd.Close()
	c.Assert(err, IsNil)
	for index := range td {
		c.Assert(utf16[index*2], Equals, td[index])
		c.Assert(utf16[index*2+1], Equals, byte(1))
	}
	c.Assert(len(utf16), Equals, 2*len(td))
}

// test that if asciiToUtf16 closes off writes early, that it returns errors properly
func (rcself *ReadFromConnSuite) TestReadFromBrokenPipe(c *C) {

	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	dataWrittenChan := make(chan int)
	go earlyReadClose(c, p1, dataWrittenChan)
	td := []byte(rcself.testData)
	rd, err := NewReadConn(p0, bytes.NewBuffer(td))
	c.Assert(err, IsNil)
	var utf16 []byte
	utf16, err = ioutil.ReadAll(rd)
	c.Assert(err, Not(IsNil))
	err = rd.Close()
	c.Assert(err, IsNil)
	dataWritten := <-dataWrittenChan
	_ = dataWritten
	xout := utf16
	var twofound, threefound bool
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

// test that if asciiToUtf16 closes off reads early, that it returns errors properly
func (rcself *ReadFromConnSuite) TestShortReadError(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	dataWrittenChan := make(chan int)
	go earlyWriteClose(c, p1, dataWrittenChan)
	td := []byte(rcself.testData)

	rd, err := NewReadConn(p0, bytes.NewBuffer([]byte(td)))
	c.Assert(err, IsNil)

	xout, oerr := ioutil.ReadAll(rd)
	err = rd.Close()
	if oerr != nil {
		c.Assert(err, IsNil)
	} else {
		c.Assert(err, Not(IsNil))
	}

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

type EarlyExitReader struct {
	firstRet  byte
	second    bool
	secondRet byte
	err       error
}

func (eeself *EarlyExitReader) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if !eeself.second {
		data[0] = eeself.firstRet
		eeself.second = true
		return 1, nil
	}
	data[0] = eeself.secondRet
	return 1, eeself.err
}
func (eeself *EarlyExitReader) Close() error {
	return nil
}

type CloseErrReader struct {
	reader io.Reader
	err    error
}

func (eeself *CloseErrReader) Read(data []byte) (int, error) {
	return eeself.reader.Read(data)
}
func (eeself *CloseErrReader) Close() error {
	return eeself.err
}

// test that if the upstream reader errors on close, that those errors are passed down to the
// toplevel Reader interface
func (rcself *ReadFromConnSuite) TestCloseErr(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	go asciiToUtf16(c, p1)
	td := []byte(rcself.testData)
	xer := errors.New("closerr")
	rd, err := NewReadConn(p0,
		&CloseErrReader{reader: bytes.NewBuffer(td),
			err: xer,
		})
	c.Assert(err, IsNil)
	var utf16 []byte
	utf16, err = ioutil.ReadAll(rd)
	c.Assert(err, IsNil)
	err = rd.Close()
	c.Assert(err, Equals, xer)
	for index := range td {
		c.Assert(utf16[index*2], Equals, td[index])
		c.Assert(utf16[index*2+1], Equals, byte(1))
	}
	c.Assert(len(utf16), Equals, 2*len(td))
}

// test if the upstream reader returns a non EOF error, that the error is reflected
// back to the caller interface
func (rcself *ReadFromConnSuite) TestEarlyReadErr(c *C) {
	p0, p1, err := MakeUnixPipe()
	c.Assert(err, IsNil)
	go asciiToUtf16(c, p1)
	xer := errors.New("readerr")
	rd, err := NewReadConn(p0,
		&EarlyExitReader{firstRet: byte(0xcd),
			secondRet: byte(0xfe),
			err:       xer,
		})
	c.Assert(err, IsNil)
	var utf16 []byte
	utf16, err = ioutil.ReadAll(rd)
	cerr := rd.Close()
	if err == nil {
		c.Assert(cerr, Equals, xer)
	} else {
		c.Assert(err, Equals, xer)
		c.Assert(cerr, IsNil)
	}
	c.Assert(len(utf16), Equals, 4)
	c.Assert(utf16[0], Equals, byte(0xcd))
	c.Assert(utf16[1], Equals, byte(0x1))
	c.Assert(utf16[2], Equals, byte(0xfe))
	c.Assert(utf16[3], Equals, byte(0x1))
}

var _ = Suite(&ReadFromConnSuite{})
