package binlog

import (
	"io"

	"github.com/dropbox/godropbox/errors"
)

type Logger struct {
	Fatalf       func(pattern string, values ...interface{})
	Infof        func(pattern string, values ...interface{})
	VerboseInfof func(pattern string, values ...interface{})
}

// EventReader is the common reader interface for all mysql binary log event
// streams.  NOTE: The EventReader interface purposely does not support
// jumping to a specific log position offset because skipping is very error
// prone.  WaitForEvent is threadsafe; none of the other methods are threadsafe.
type EventReader interface {
	// NextEvent returns the next available event from the event stream.
	NextEvent() (Event, error)

	// Close closes the reader.  Subsequent calls to NextEvent will return
	// an error.
	Close() error

	// peekHeaderBytes returns up to sizeOfBasicV4EventHeader number of bytes
	// from the event header.  This is used for checking binlog magic marker
	// and format version at the beginning of the event stream.
	peekHeaderBytes(numBytes int) ([]byte, error)

	// consumeHeaderBytes will discard up to sizeOfBasicV4EventHeader number
	// of bytes from the event header.  This is used for throwing away the
	// binlog magic marker at the beginning of the event stream.
	consumeHeaderBytes(numBytes int) error

	// nextEventEndPosition returns the next event's end position relative to
	// the source io stream.  Note the value is an underestimate when the
	// next event's common header is not yet parsed.
	nextEventEndPosition() int64
}

// When tailing logs on a mysql box, there's a potential race conditions where
// the rotate event is written, but a new log file is not created yet.  It's
// probably safe to retry when this occurs (before quitting).
type FailedToOpenFileError struct {
	errors.DropboxError
	LogFileNum uint
}

// This returns true if the error returned by the event parser is retryable.
func IsRetryableError(err error) bool {
	if err == io.EOF {
		return true
	}
	if _, ok := err.(*FailedToOpenFileError); ok {
		return true
	}

	return false
}
