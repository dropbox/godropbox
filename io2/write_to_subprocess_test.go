package io2

import (
	"bytes"
	"errors"

	. "gopkg.in/check.v1"
	"os/exec"

	"strings"
)

type WriteToSubprocessSuite struct {
	testData string
}

func (wrself *WriteToSubprocessSuite) SetUpTest(c *C) {
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

type WrappedBuffer struct {
	bytes.Buffer
	isClosed bool
}

func (wbself *WrappedBuffer) Close() error {
	wbself.isClosed = true
	return nil
}

type ErrorWrappedBuffer struct {
	bytes.Buffer
	isClosed bool
}

var ShouldFail = errors.New("Should fail")

func (wbself *ErrorWrappedBuffer) Close() error {
	wbself.isClosed = true
	return ShouldFail
}

func (wrself *WriteToSubprocessSuite) TestSubprocessGzipCompression(c *C) {
	var compressedOutput WrappedBuffer
	wr, err := NewWriteSubprocess(exec.Command("gzip"), &compressedOutput)
	c.Assert(err, IsNil)
	wr.Write([]byte(wrself.testData))
	wr.Close()
	var finalOutput bytes.Buffer
	dwr, err := NewWriteSubprocess(exec.Command("gunzip"), &finalOutput)
	c.Assert(err, IsNil)
	_, err = dwr.Write(compressedOutput.Bytes())
	c.Assert(err, IsNil)
	dwr.Close()
	c.Assert(string(finalOutput.Bytes()), Equals, wrself.testData)
	c.Assert(len(compressedOutput.Bytes()), Not(Equals), len(wrself.testData))
	c.Assert(compressedOutput.isClosed, Equals, true)
}

func (wrself *WriteToSubprocessSuite) TestSubprocessGzipCompressionCloseErr(c *C) {
	var compressedOutput ErrorWrappedBuffer
	wr, err := NewWriteSubprocess(exec.Command("gzip"), &compressedOutput)
	c.Assert(err, IsNil)
	_, err = wr.Write([]byte(wrself.testData))
	c.Assert(err, IsNil)
	err = wr.Close()
	c.Assert(err, Equals, ShouldFail)
	c.Assert(compressedOutput.isClosed, Equals, true)

	var finalOutput bytes.Buffer
	dwr, err := NewWriteSubprocess(exec.Command("gunzip"), &finalOutput)
	c.Assert(err, IsNil)
	_, err = dwr.Write(compressedOutput.Bytes())
	c.Assert(err, IsNil)
	err = dwr.Close()
	c.Assert(err, IsNil)
	c.Assert(string(finalOutput.Bytes()), Equals, wrself.testData)
	c.Assert(len(compressedOutput.Bytes()), Not(Equals), len(wrself.testData))
}

var _ = Suite(&WriteToSubprocessSuite{})
