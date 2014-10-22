package binlog

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dropbox/godropbox/errors"
)

type MockMultifileReader struct {
	reader           EventReader
	files            [][]byte
	currentFileIndex int
	isClosed         bool
}

var _ EventReader = &MockMultifileReader{}

func newMockReader(file []byte) EventReader {
	return NewParsedV4EventReader(
		NewRawV4EventReader(bytes.NewBuffer(file), "fake"),
		NewV4EventParserMap())
}

func NewMockMultifileReader(files [][]byte) *MockMultifileReader {
	var reader EventReader
	if len(files) == 0 {
		reader = nil
	} else {
		reader = newMockReader(files[0])
	}

	return &MockMultifileReader{
		reader:           reader,
		files:            files,
		currentFileIndex: 0,
		isClosed:         false,
	}
}

func (r *MockMultifileReader) getReader() (EventReader, error) {
	if r.isClosed {
		return nil, errors.Newf("MockMultifileReader is closed")
	}

	if r.reader == nil {
		if r.currentFileIndex < len(r.files) {
			r.reader = newMockReader(r.files[r.currentFileIndex])
		} else {
			return nil, io.EOF
		}
	}

	return r.reader, nil
}

func (r *MockMultifileReader) peekHeaderBytes(numBytes int) ([]byte, error) {
	reader, err := r.getReader()
	if err != nil {
		return nil, err
	}
	return reader.peekHeaderBytes(numBytes)
}

func (r *MockMultifileReader) consumeHeaderBytes(numBytes int) error {
	reader, err := r.getReader()
	if err != nil {
		return err
	}
	return reader.consumeHeaderBytes(numBytes)
}

func (r *MockMultifileReader) nextEventEndPosition() int64 {
	reader, err := r.getReader()
	if err != nil {
		panic(
			fmt.Sprintf("Cannot find reader for MockMultifileReader: %v", err))
	}
	return reader.nextEventEndPosition()
}

func (r *MockMultifileReader) Close() error {
	r.isClosed = true
	if r.reader == nil {
		return nil
	}

	err := r.reader.Close()
	r.reader = nil
	return err
}

func (r *MockMultifileReader) NextEvent() (Event, error) {
	for {
		reader, err := r.getReader()
		if err != nil {
			return nil, err
		}

		event, err := reader.NextEvent()
		if err == nil {
			return event, nil
		} else if err == io.EOF {
			r.currentFileIndex++
			r.reader = nil
		} else {
			return nil, err
		}
	}
}
