package binlog

import (
	"io"
	"sync"
)

type MockLogFile struct {
	logBuffer []byte

	mu sync.Mutex
}

type MockLogFileReader struct {
	file   *MockLogFile
	cursor int
}

// Statically verify that MockLogFile implements io.Reader.

var _ io.Reader = &MockLogFileReader{}

func NewMockLogFile() *MockLogFile {
	return &MockLogFile{
		logBuffer: make([]byte, 0),
	}
}

func (mlf *MockLogFile) GetReader() *MockLogFileReader {
	return newMockLogFileReader(mlf)
}

func (mlf *MockLogFile) Write(contents []byte) {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()
	mlf.logBuffer = append(mlf.logBuffer, contents...)
}

func newMockLogFileReader(logFile *MockLogFile) *MockLogFileReader {
	return &MockLogFileReader{
		file:   logFile,
		cursor: 0,
	}
}

func (reader *MockLogFileReader) Read(p []byte) (n int, err error) {
	reader.file.mu.Lock()
	defer reader.file.mu.Unlock()

	if reader.cursor+len(p) > len(reader.file.logBuffer) {
		// We can't read anything.
		return 0, io.EOF
	}

	// We can read something.
	copied := copy(p, reader.file.logBuffer[reader.cursor:])
	if copied != len(p) {
		panic("MockLogFileReader read failed")
	}
	reader.cursor += len(p)
	return len(p), nil
}
