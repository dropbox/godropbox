package binlog

import (
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/dropbox/godropbox/errors"
)

type InvalidRotationError struct {
	errors.DropboxError
}

const maxLogFileNum = 999999

type LogFileReaderCreator func(
	dir string,
	file string,
	parsers V4EventParserMap) (
	EventReader,
	error)

type logStreamV4EventReader struct {
	isClosed bool

	logDirectory   string
	logPrefix      string
	nextLogFileNum uint

	isRelayLog bool

	isNewLogFile bool

	reader  EventReader
	parsers V4EventParserMap

	newLogFileReader LogFileReaderCreator

	logger Logger
}

// This returns an EventReader which read and parses events from a (bin /relay)
// log stream composed of multiple log files.  If no parser is available for
// the event, or if an error occurs during parsing, then the reader will
// return the original event along with the error.  NOTE: this reader will
// transparently switch log files on rotate events (relay log wrapper events
// are not returned).  When the reader fails to open a log file, it will return
// a *FailedToOpenFileError; it is safe to retry reading, assuming the filename
// is valid.  When the reader encounters an invalid rotate event, it will
// return both the rotate event and an *InvalidRotationError.
func NewLogStreamV4EventReader(
	logDirectory string,
	logPrefix string,
	startingLogFileNum uint,
	isRelayLog bool,
	logger Logger) EventReader {

	openLogReader := func(
		dir string,
		file string,
		parsers V4EventParserMap) (
		EventReader,
		error) {

		filePath := path.Join(dir, file)
		logFile, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}

		return NewLogFileV4EventReader(logFile, filePath, parsers, logger), nil
	}

	return NewLogStreamV4EventReaderWithLogFileReaderCreator(
		logDirectory,
		logPrefix,
		startingLogFileNum,
		isRelayLog,
		logger,
		openLogReader)
}

func NewLogStreamV4EventReaderWithLogFileReaderCreator(
	logDirectory string,
	logPrefix string,
	startingLogFileNum uint,
	isRelayLog bool,
	logger Logger,
	newLogFileReader LogFileReaderCreator) EventReader {

	return &logStreamV4EventReader{
		logDirectory:     logDirectory,
		logPrefix:        logPrefix,
		nextLogFileNum:   startingLogFileNum,
		isRelayLog:       isRelayLog,
		reader:           nil,
		parsers:          NewV4EventParserMap(),
		newLogFileReader: newLogFileReader,
		logger:           logger,
	}
}

func (r *logStreamV4EventReader) getLogFileReader() (EventReader, error) {
	if r.isClosed {
		return nil, errors.New("reader is closed")
	}

	if r.reader != nil {
		return r.reader, nil
	}

	logFilename := r.logPrefix + fmt.Sprintf("%06d", r.nextLogFileNum)
	r.logger.Infof(
		"Opening log file: %s",
		path.Join(r.logDirectory, logFilename))

	reader, err := r.newLogFileReader(r.logDirectory, logFilename, r.parsers)
	if err != nil {
		_, ok := err.(*FailedToOpenFileError)
		if ok {
			return nil, err
		}
		derr, ok := err.(errors.DropboxError)
		if ok {
			return nil, &FailedToOpenFileError{
				DropboxError: derr,
				LogFileNum:   r.nextLogFileNum,
			}
		}
		return nil, &FailedToOpenFileError{
			DropboxError: errors.Wrap(err, "Failed to open file"),
			LogFileNum:   r.nextLogFileNum,
		}
	}

	r.isNewLogFile = true
	r.reader = reader
	return reader, nil
}

func (r *logStreamV4EventReader) peekHeaderBytes(numBytes int) ([]byte, error) {
	reader, err := r.getLogFileReader()
	if err != nil {
		return nil, err
	}
	return reader.peekHeaderBytes(numBytes)
}

func (r *logStreamV4EventReader) consumeHeaderBytes(numBytes int) error {
	reader, err := r.getLogFileReader()
	if err != nil {
		return err
	}
	return reader.consumeHeaderBytes(numBytes)
}

func (r *logStreamV4EventReader) nextEventEndPosition() int64 {
	r.logger.Fatalf(
		"nextEventEndPosition is invalid for logStreamV4EventReader")
	return -1
}

const logFileNumberLength = 6 // zero prefix padded

func (r *logStreamV4EventReader) NextEvent() (Event, error) {
	reader, err := r.getLogFileReader()
	if err != nil {
		return nil, err
	}

	event, err := reader.NextEvent()
	if err != nil {
		return event, err
	}

	if r.isNewLogFile {
		r.isNewLogFile = false

		if r.isRelayLog {
			// Sanity check
			_, ok := event.(*FormatDescriptionEvent)
			if !ok {
				return nil, errors.Newf("Invalid relay log event file")
			}

			// Skip the relay log wrapper FDE.
			return r.NextEvent()
		}
	}

	rotate, isRotate := event.(*RotateEvent)
	_, isStop := event.(*StopEvent)
	if !isStop && !isRotate {
		return event, nil
	}

	nextFileNum := -1
	if isRotate {
		// In case of rotate event we can do extra verification
		if len(rotate.NewLogName())-logFileNumberLength == len(r.logPrefix) &&
			string(rotate.NewLogName()[:len(r.logPrefix)]) == r.logPrefix {

			numString := string(rotate.NewLogName()[len(r.logPrefix):])
			nextFileNum, err = strconv.Atoi(numString)
			if err != nil {
				return event, &InvalidRotationError{
					errors.Newf(
						"Invalid log rotation.  Failed to parse log number.  "+
							"Rotate event log file: %s (curent file: %s%06d)",
						string(rotate.NewLogName()),
						r.logPrefix,
						r.nextLogFileNum),
				}
			}
		}

		if nextFileNum < 0 {
			if r.isRelayLog {
				// Relay log contains rotate events from the master's binlog.
				r.logger.Infof(
					"Ignored master's rotate event. "+
						"Rotate event log file: %s (curent file: %s%06d)",
					string(rotate.NewLogName()),
					r.logPrefix,
					r.nextLogFileNum)
				return event, nil
			}

			return event, &InvalidRotationError{
				errors.Newf(
					"Invalid log rotation.  Unexpected new log file.  "+
						"New log file: %s (current file: %s%06d)",
					string(rotate.NewLogName()),
					r.logPrefix,
					r.nextLogFileNum),
			}
		}

		logNumJumped := false
		if r.nextLogFileNum < maxLogFileNum {
			if nextFileNum != int(r.nextLogFileNum+1) {
				logNumJumped = true
			}
		} else {
			if nextFileNum != 0 {
				logNumJumped = true
			}
		}

		if logNumJumped {
			return event, &InvalidRotationError{
				errors.Newf(
					"Invalid log rotation.  Unexpected log file number.  "+
						"New log file: %s (current file: %s%06d)",
					string(rotate.NewLogName()),
					r.logPrefix,
					r.nextLogFileNum),
			}
		}

		r.logger.Infof(
			"Reached end of file %s%d. Next log file is: %s",
			r.logPrefix,
			r.nextLogFileNum,
			string(rotate.NewLogName()))
	} else {
		// We need to manually compute next filenum
		if r.nextLogFileNum < maxLogFileNum {
			nextFileNum = int(r.nextLogFileNum + 1)
		} else {
			nextFileNum = 0
		}

		r.logger.Infof(
			"Detected stop event in %s%d. Next log file is: %s%d",
			r.logPrefix,
			r.nextLogFileNum,
			r.logPrefix,
			nextFileNum)
	}

	err = r.reader.Close()
	r.reader = nil
	r.nextLogFileNum = uint(nextFileNum)

	if r.isRelayLog && isRotate {
		// Skip the relay log wrapper rotation event.
		return r.NextEvent()
	}

	return event, err
}

func (r *logStreamV4EventReader) Close() error {
	r.isClosed = true

	if r.reader == nil {
		return nil
	}

	err := r.reader.Close()
	r.reader = nil
	return err
}
