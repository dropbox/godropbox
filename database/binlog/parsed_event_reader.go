package binlog

import (
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type parsedV4EventReader struct {
	reader       EventReader
	eventParsers V4EventParserMap
}

// This returns an EventReader which applies the appropriate parser on each
// raw v4 event in the stream.  If no parser is available for the event,
// or if an error occurs during parsing, then the reader will return the
// original event along with the error.
func NewParsedV4EventReader(
	reader EventReader,
	parsers V4EventParserMap) EventReader {

	return &parsedV4EventReader{
		reader:       reader,
		eventParsers: parsers,
	}
}

func (r *parsedV4EventReader) peekHeaderBytes(numBytes int) ([]byte, error) {
	return r.reader.peekHeaderBytes(numBytes)
}

func (r *parsedV4EventReader) consumeHeaderBytes(numBytes int) error {
	return r.reader.consumeHeaderBytes(numBytes)
}

func (r *parsedV4EventReader) nextEventEndPosition() int64 {
	return r.reader.nextEventEndPosition()
}

func (r *parsedV4EventReader) Close() error {
	return r.reader.Close()
}

func (r *parsedV4EventReader) NextEvent() (Event, error) {
	event, err := r.reader.NextEvent()
	if err != nil {
		return event, err
	}

	raw, ok := event.(*RawV4Event)
	if !ok {
		return event, nil // don't try to parse non-raw events
	}

	extraHeadersSize := 0
	checksumSize := 0
	if raw.EventType() != mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT {
		extraHeadersSize = r.eventParsers.ExtraHeadersSize()
		checksumSize = r.eventParsers.ChecksumSize()
	}
	err = raw.SetExtraHeadersSize(extraHeadersSize)
	if err != nil {
		return event, err // return both raw event and error
	}
	err = raw.SetChecksumSize(checksumSize)
	if err != nil {
		return event, err // return both raw event and error
	}

	parser := r.eventParsers.Get(raw.EventType())
	if parser == nil {
		return event, nil // no parser available, just return the raw event
	}

	err = raw.SetFixedLengthDataSize(parser.FixedLengthDataSize())
	if err != nil {
		return event, err // return both raw event and error
	}

	event, err = parser.Parse(raw)
	if err != nil {
		return event, err
	}

	tm, ok := event.(*TableMapEvent)
	if ok {
		r.eventParsers.SetTableContext(tm)
	}

	return event, nil
}
