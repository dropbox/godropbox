package binlog

import (
	"fmt"
	"io"

	"github.com/dropbox/godropbox/errors"
)

// Assumes that all files returned by successive calls to MockFileFetcher are the same,
// except perhaps the last MockLogFile may have additional content in a later call, and later calls
// may return more mock files. These assumptions should be reasonable for an append-only log model.
type MockFileFetcher func() []*MockLogFile

type MockMultifileReader struct {
	reader           EventReader
	fetchFiles       MockFileFetcher
	currentFileIndex int
	isClosed         bool
}

var _ EventReader = &MockMultifileReader{}

func newMockReader(file *MockLogFile) EventReader {
	return NewParsedV4EventReader(
		NewRawV4EventReader(file.GetReader(), "fake"),
		NewV4EventParserMap())
}

// There may be more files over time, so the MockFileFetcher is used instead of a static slice of
// *MockLogFile.
func NewMockMultifileReader(fetchFiles MockFileFetcher) *MockMultifileReader {
	var reader EventReader
	files := fetchFiles()
	if len(files) == 0 {
		reader = nil
	} else {
		reader = newMockReader(files[0])
	}

	return &MockMultifileReader{
		reader:           reader,
		fetchFiles:       fetchFiles,
		currentFileIndex: 0,
		isClosed:         false,
	}
}

func (r *MockMultifileReader) getReader() (EventReader, error) {
	if r.isClosed {
		return nil, errors.Newf("MockMultifileReader is closed")
	}

	if r.reader == nil {
		files := r.fetchFiles()

		if r.currentFileIndex < len(files) {
			r.reader = newMockReader(files[r.currentFileIndex])
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
		} else if err == io.EOF && r.currentFileIndex+1 < len(r.fetchFiles()) {
			// There's another file, so we'll try again.
			r.currentFileIndex++
			r.reader = nil
		} else {
			return nil, err
		}
	}
}
