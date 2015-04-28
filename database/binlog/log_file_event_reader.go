package binlog

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

var logFileMagic = []byte("\xfe\x62\x69\x6e")

const (
	sizeOfFormatVersionHeader = 13 // sizeof(formatCheckHeader)
	maxFirstEventLengthForV1  = 75
)

// formatVersionHeader is only used for checking the binlog format version.  See
// http://dev.mysql.com/doc/internals/en/determining-binary-log-version.html
// for additional details.
type formatVersionHeader struct {
	Timestamp   uint32 // ignored by version check
	EventType   uint8
	ServerId    uint32 // ignored by version check
	EventLength uint32
}

func (h *formatVersionHeader) version() int {
	eventType := mysql_proto.LogEventType_Type(h.EventType)
	if eventType == mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT {
		return 4
	} else if eventType == mysql_proto.LogEventType_START_EVENT_V3 {
		if h.EventLength >= maxFirstEventLengthForV1 {
			return 3
		}
		return 1
	}

	return 3
}

type logFileV4EventReader struct {
	reader                      EventReader
	parsers                     V4EventParserMap
	passedMagicBytesCheck       bool
	passedLogFormatVersionCheck bool
	logger                      Logger
}

// This returns an EventReader which read events from a single (bin / relay)
// log file, with appropriate parser applied on each event.  If no parser is
// available for the event, or if an error occurs during parsing, then the
// reader will return the original event along with the error.  NOTE: this
// reader is responsible for checking the log file magic marker, the binlog
// format version and all format description events within the stream.  It is
// also responsible for setting the checksum size for non-FDE events.
func NewLogFileV4EventReader(
	src io.Reader,
	srcName string,
	parsers V4EventParserMap,
	logger Logger) EventReader {

	rawReader := NewRawV4EventReader(src, srcName)

	return &logFileV4EventReader{
		reader:                      NewParsedV4EventReader(rawReader, parsers),
		parsers:                     parsers,
		passedMagicBytesCheck:       false,
		passedLogFormatVersionCheck: false,
		logger: logger,
	}
}

func (r *logFileV4EventReader) peekHeaderBytes(numBytes int) ([]byte, error) {
	return r.reader.peekHeaderBytes(numBytes)
}

func (r *logFileV4EventReader) consumeHeaderBytes(numBytes int) error {
	return r.reader.consumeHeaderBytes(numBytes)
}

func (r *logFileV4EventReader) nextEventEndPosition() int64 {
	return r.reader.nextEventEndPosition()
}

func (r *logFileV4EventReader) Close() error {
	return r.reader.Close()
}

func (r *logFileV4EventReader) maybeCheckMagicBytes() error {
	if r.passedMagicBytesCheck {
		return nil
	}

	magicBytes, err := r.peekHeaderBytes(len(logFileMagic))
	if err != nil {
		return err
	}

	if bytes.Compare(logFileMagic, magicBytes) != 0 {
		return errors.New("Invalid binary log magic marker")
	}

	err = r.consumeHeaderBytes(len(logFileMagic))
	if err != nil {
		return err
	}

	r.passedMagicBytesCheck = true
	return nil
}

func (r *logFileV4EventReader) maybeCheckLogFormatVersion() error {
	if r.passedLogFormatVersionCheck {
		return nil
	}

	headerBytes, err := r.peekHeaderBytes(sizeOfFormatVersionHeader)
	if err != nil {
		return err
	}

	header := formatVersionHeader{}

	_, err = readLittleEndian(headerBytes, &header)
	if err != nil {
		return err
	}

	if version := header.version(); version != 4 {
		return errors.Newf(
			"Binary log reader does not support V%d binlog format",
			version)
	}

	r.passedLogFormatVersionCheck = true
	return nil
}

func (r *logFileV4EventReader) checkFDE(fde *FormatDescriptionEvent) error {
	if fde.BinlogVersion() != 4 {
		return errors.Newf(
			"Invalid binlog format version: %d",
			fde.BinlogVersion())
	}

	if fde.ExtraHeadersSize() != 0 {
		return errors.Newf(
			"Invalid extra headers size: %d",
			fde.ExtraHeadersSize())
	}

	alg := fde.ChecksumAlgorithm()
	if alg != mysql_proto.ChecksumAlgorithm_OFF &&
		alg != mysql_proto.ChecksumAlgorithm_CRC32 {

		return errors.Newf(
			"Invalid checksum algorithm: %d (%s)",
			alg,
			alg.String())
	}

	errMsg := ""
	for i := 0; i < fde.NumKnownEventTypes(); i++ {
		t := mysql_proto.LogEventType_Type(i)

		if t == mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT {
			actual := fde.FixedLengthDataSizeForType(t)
			if actual != FDEFixedLengthDataSizeFor56 &&
				actual != FDEFixedLengthDataSizeFor55 {

				errMsg += fmt.Sprintf(
					"%s (expected: %d (5.6) or %d (5.5) actual: %d); ",
					t.String(),
					FDEFixedLengthDataSizeFor56,
					FDEFixedLengthDataSizeFor55,
					actual)
			}
		} else {
			parser := r.parsers.Get(t)
			if parser == nil {
				continue
			}

			expected := parser.FixedLengthDataSize()
			actual := fde.FixedLengthDataSizeForType(t)
			if expected != actual {
				errMsg += fmt.Sprintf(
					"%s (expected: %d actual: %d); ",
					t.String(),
					expected,
					actual)
			}
		}
	}

	if errMsg != "" {
		return errors.New("Invalid fixed length data size: " + errMsg)
	}

	return nil
}

func (r *logFileV4EventReader) NextEvent() (Event, error) {
	err := r.maybeCheckMagicBytes()
	if err != nil {
		return nil, err
	}

	err = r.maybeCheckLogFormatVersion()
	if err != nil {
		return nil, err
	}

	event, err := r.reader.NextEvent()
	if err != nil {
		return event, err
	}

	fde, ok := event.(*FormatDescriptionEvent)
	if !ok {
		return event, nil // just return the non-FDE event
	}

	// Always set checksum size, even when fde check fails.
	// TODO(patrick): revisit this if it becomes an issue.
	checksumSize := 0
	if fde.ChecksumAlgorithm() == mysql_proto.ChecksumAlgorithm_CRC32 {
		checksumSize = 4
	}
	r.logger.VerboseInfof("Setting event checksum size to %d", checksumSize)
	r.parsers.SetChecksumSize(checksumSize)

	r.logger.VerboseInfof(
		"Setting # of supported event types to %d",
		fde.NumKnownEventTypes())
	r.parsers.SetNumSupportedEventTypes(fde.NumKnownEventTypes())

	return fde, r.checkFDE(fde)
}
