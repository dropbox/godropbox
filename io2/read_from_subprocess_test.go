package io2

import (
	"bytes"
	. "gopkg.in/check.v1"
	"io/ioutil"
	"os/exec"
	"strings"
)

type ReadFromSubprocessSuite struct {
	testData string
}

func (rdself *ReadFromSubprocessSuite) SetUpTest(c *C) {
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
	rdself.testData = strings.Join(localWords, " _ ")

}

func (rdself *ReadFromSubprocessSuite) TestGzip(c *C) {
	rawInput := bytes.NewBuffer([]byte(rdself.testData))

	rd, err := NewReadSubprocess(exec.Command("gzip"), rawInput)
	c.Assert(err, IsNil)
	intermediateOutput, ioerr := ioutil.ReadAll(&rd)
	c.Assert(ioerr, IsNil)
	ioerr = rd.Close()
	c.Assert(ioerr, IsNil)

	drd, derr := NewReadSubprocess(exec.Command("gunzip"),
		bytes.NewBuffer(intermediateOutput))
	c.Assert(derr, IsNil)
	finalOutput, dioerr := ioutil.ReadAll(&drd)
	c.Assert(dioerr, IsNil)
	dioerr = drd.Close()
	c.Assert(dioerr, IsNil)
	c.Assert(string(finalOutput), Equals, rdself.testData)
	c.Assert(len(intermediateOutput), Not(Equals), len(rdself.testData))
	c.Assert(len(intermediateOutput) < len(rdself.testData), Equals, true)
}

var _ = Suite(&ReadFromSubprocessSuite{})
