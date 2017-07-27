package io2

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
	"strings"

	. "gopkg.in/check.v1"
)

type ReaderToWriterAdapterSuite struct {
	testData string
}

func (rwself *ReaderToWriterAdapterSuite) SetUpTest(c *C) {
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
	rwself.testData = strings.Join(localWords, " _ ")
}

func (rwself *ReaderToWriterAdapterSuite) TestZlibPipeline(c *C) {
	input := bytes.NewBuffer([]byte(rwself.testData))
	var compressedOutput bytes.Buffer
	compressor, rwaerr := NewReaderToWriterAdapter(
		func(a io.Writer) (io.Writer, error) { ret := zlib.NewWriter(a); return ret, nil },
		input)
	c.Assert(rwaerr, IsNil)
	skip := 1
	ci := input.Bytes()
	var ioerr error
	for ioerr == nil {
		if skip > len(ci) {
			skip = len(ci)
		}
		buffer := make([]byte, skip)
		var count int
		count, ioerr = compressor.Read(buffer)
		_, _ = compressedOutput.Write(buffer[:count])
		if ioerr != io.EOF {
			c.Assert(ioerr, IsNil)
		}
		skip %= 128
		skip += 1
	}

	compressedForm := compressedOutput.Bytes()

	r, err := zlib.NewReader(bytes.NewBuffer(compressedForm))
	c.Assert(err, IsNil)
	var finalOutput bytes.Buffer
	_, err = io.Copy(&finalOutput, r)
	c.Assert(err, IsNil)
	ioerr = compressor.Close()
	c.Assert(ioerr, IsNil)
	c.Assert(string(finalOutput.Bytes()), Equals, rwself.testData)
}

type DoubleZlibWriter struct {
	firstZlib  io.WriteCloser
	secondZlib io.WriteCloser
}

func NewDoubleZlibWriter(output io.Writer) DoubleZlibWriter {
	dzw := DoubleZlibWriter{firstZlib: zlib.NewWriter(output)}
	dzw.secondZlib = zlib.NewWriter(dzw.firstZlib)
	return dzw
}

func (dzself *DoubleZlibWriter) Write(data []byte) (int, error) {
	return dzself.secondZlib.Write(data)
}

func (dzself *DoubleZlibWriter) Close() error {
	err := dzself.secondZlib.Close()
	err2 := dzself.firstZlib.Close()
	if err != nil {
		return err
	}
	return err2
}

func (rwself *ReaderToWriterAdapterSuite) TestDoubleZlibPipeline(c *C) {
	input := bytes.NewBuffer([]byte(rwself.testData))
	var compressedOutput bytes.Buffer
	compressor, rwaerr := NewReaderToWriterAdapter(
		func(a io.Writer) (io.Writer, error) {
			ret := NewDoubleZlibWriter(a)
			return &ret, nil
		},
		input)
	c.Assert(rwaerr, IsNil)
	skip := 1
	ci := input.Bytes()
	var ioerr error
	for ioerr == nil {
		if skip > len(ci) {
			skip = len(ci)
		}
		buffer := make([]byte, skip)
		var count int
		count, ioerr = compressor.Read(buffer)
		_, _ = compressedOutput.Write(buffer[:count])
		if ioerr != io.EOF {
			c.Assert(ioerr, IsNil)
		}
		skip %= 511
		skip += 1
	}

	compressedForm := compressedOutput.Bytes()

	r, err := zlib.NewReader(bytes.NewBuffer(compressedForm))
	c.Assert(err, IsNil)
	r, err = zlib.NewReader(r)
	c.Assert(err, IsNil)
	var finalOutput bytes.Buffer
	_, err = io.Copy(&finalOutput, r)
	c.Assert(err, IsNil)
	ioerr = compressor.Close()
	c.Assert(ioerr, IsNil)
	c.Assert(string(finalOutput.Bytes()), Equals, rwself.testData)
}

var _ = Suite(&ReaderToWriterAdapterSuite{})

func (rwself *ReaderToWriterAdapterSuite) TestEarlyError(c *C) {
	err := errors.New("Just kidding")

	_, rwaerr := NewReaderToWriterAdapter(
		func(a io.Writer) (io.Writer, error) { return nil, err },
		bytes.NewBuffer([]byte(rwself.testData)))
	c.Assert(rwaerr, Equals, err)
}

type ErrorWriter struct {
}

var doa error = errors.New("DOA")

func (ewself ErrorWriter) Write(data []byte) (int, error) {
	return 0, doa
}

func (ewself ErrorWriter) Close(data []byte) error {
	return nil
}

func (rwself *ReaderToWriterAdapterSuite) TestFirstError(c *C) {
	input := bytes.NewBuffer([]byte(rwself.testData))
	compressor, rwaerr := NewReaderToWriterAdapter(
		func(a io.Writer) (io.Writer, error) { ret := ErrorWriter{}; return ret, nil },
		input)
	c.Assert(rwaerr, IsNil)
	skip := 1
	var ioerr error
	buffer := make([]byte, skip)
	_, ioerr = compressor.Read(buffer)
	c.Assert(ioerr, Equals, doa)
}

type CountErrorWriter struct {
	count      int
	downstream io.Writer
}

func (ewself *CountErrorWriter) Write(data []byte) (int, error) {
	if ewself.count == 0 {
		return 0, doa
	} else {
		ewself.count -= 1
		ewself.downstream.Write(data)
		return len(data), nil
	}
}

func (ewself *CountErrorWriter) Close() error {
	if ewself.count <= -1 {
		return doa
	}
	return nil
}

func (rwself *ReaderToWriterAdapterSuite) TestMidError(c *C) {
	input := bytes.NewBuffer([]byte(rwself.testData))
	var compressedOutput bytes.Buffer
	compressor, rwaerr := NewReaderToWriterAdapter(
		func(a io.Writer) (io.Writer, error) {
			ret := CountErrorWriter{count: 1, downstream: a}
			return &ret, nil
		},
		input)
	c.Assert(rwaerr, IsNil)
	skip := 1
	ci := input.Bytes()
	var ioerr error
	okFound := false
	badFound := false
	for ioerr == nil {
		if skip > len(ci) {
			skip = len(ci)
		}
		buffer := make([]byte, skip)
		var count int
		count, ioerr = compressor.Read(buffer)
		_, _ = compressedOutput.Write(buffer[:count])
		if ioerr != io.EOF {
			if ioerr == nil {
				okFound = true
			}
			if ioerr == doa {
				badFound = true
			}
		}
		skip %= 128
		skip += 1
	}
	c.Assert(okFound, Equals, true)
	c.Assert(badFound, Equals, true)
	ioerr = compressor.Close()
	c.Assert(ioerr, IsNil)
}

func (rwself *ReaderToWriterAdapterSuite) TestFinalError(c *C) {
	input := bytes.NewBuffer([]byte(rwself.testData))
	var compressedOutput bytes.Buffer
	compressor, rwaerr := NewReaderToWriterAdapter(
		func(a io.Writer) (io.Writer, error) {
			ret := CountErrorWriter{count: -1, downstream: a}
			return &ret, nil
		},
		input)
	c.Assert(rwaerr, IsNil)
	skip := 1
	ci := input.Bytes()
	var ioerr error
	okFound := false
	badFound := false
	for ioerr == nil {
		if skip > len(ci) {
			skip = len(ci)
		}
		buffer := make([]byte, skip)
		var count int
		count, ioerr = compressor.Read(buffer)
		_, _ = compressedOutput.Write(buffer[:count])
		if ioerr != io.EOF {
			if ioerr == nil {
				okFound = true
			}
			if ioerr == doa {
				badFound = true
			}
		}
		skip %= 128
		skip += 1
	}
	c.Assert(okFound, Equals, true)
	c.Assert(badFound, Equals, true)
	ioerr = compressor.Close()
	c.Assert(ioerr, IsNil)
}
