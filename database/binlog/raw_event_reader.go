package binlog

import (
	"io"

	"github.com/dropbox/godropbox/bufio2"
	"github.com/dropbox/godropbox/errors"
)

type rawV4EventReader struct {
	src             io.Reader
	srcName         string
	logPosition     int64
	rawHeaderBuffer []byte
	isClosed        bool

	// NOTE: new nextEvent, headerBuffer and bodyBuffer are allocated for each
	// event.  The pointers are reset to nil upon successfully parsing an event.
	nextEvent    *RawV4Event
	headerBuffer *bufio2.LookAheadBuffer
	bodyBuffer   *bufio2.LookAheadBuffer
}

// This returns an EventReader which extracts entries from the src event stream
// and returns the entries as RawV4Event objects.  The src stream can be a
// binary log event stream or a relay log event stream.  NOTE: This reader
// assumes there is no binlog magic marker at the beginning of the stream.
// It also assumes the event entries are serialized using v4 binlog format.
// Finally, this reader does not set sizes for extra headers, fixed length data,
// and checksum (i.e. event.VariableLengthData() will return the entire event
// payload).
func NewRawV4EventReader(src io.Reader, srcName string) EventReader {
	buf := make([]byte, sizeOfBasicV4EventHeader, sizeOfBasicV4EventHeader)
	return &rawV4EventReader{
		src:             src,
		srcName:         srcName,
		logPosition:     0,
		rawHeaderBuffer: buf,
		isClosed:        false,
		headerBuffer:    nil,
		bodyBuffer:      nil,
		nextEvent:       nil,
	}
}

func (r *rawV4EventReader) getHeaderBuffer() *bufio2.LookAheadBuffer {
	if r.headerBuffer == nil { // New event
		r.headerBuffer = bufio2.NewLookAheadBufferUsing(
			r.src,
			r.rawHeaderBuffer)
	}
	return r.headerBuffer
}

func (r *rawV4EventReader) peekHeaderBytes(numBytes int) ([]byte, error) {
	return r.getHeaderBuffer().Peek(numBytes)
}

func (r *rawV4EventReader) consumeHeaderBytes(numBytes int) error {
	if r.bodyBuffer != nil {
		return errors.New(
			"Cannot consume header bytes while parsing an event's body")
	}

	err := r.getHeaderBuffer().Consume(numBytes)
	if err != nil {
		return err
	}
	r.logPosition += int64(numBytes)
	return nil
}

func (r *rawV4EventReader) nextEventEndPosition() int64 {
	if r.nextEvent == nil ||
		r.nextEvent.header.EventLength < sizeOfBasicV4EventHeader {

		return r.logPosition + sizeOfBasicV4EventHeader
	}

	return r.logPosition + int64(r.nextEvent.header.EventLength)
}

func (r *rawV4EventReader) Close() error {
	r.isClosed = true
	return nil
}

func (r *rawV4EventReader) NextEvent() (Event, error) {
	if r.isClosed {
		return nil, errors.New("Event reader is closed")
	}

	if r.nextEvent == nil { // new event
		r.nextEvent = &RawV4Event{
			sourceName:     r.srcName,
			sourcePosition: r.logPosition,
		}
	}

	if r.nextEvent.data == nil { // still parsing the header
		headerBytes, err := r.getHeaderBuffer().PeekAll()
		if err != nil {
			return nil, err
		}

		_, err = readLittleEndian(headerBytes, &r.nextEvent.header)
		if err != nil {
			return nil, err
		}

		bodySize := int(r.nextEvent.EventLength()) - sizeOfBasicV4EventHeader
		if bodySize < 0 { // should never happen
			return nil, errors.New("Invalid event size")
		}

		r.nextEvent.data = make(
			[]byte,
			r.nextEvent.EventLength(),
			r.nextEvent.EventLength())

		copied := copy(r.nextEvent.data, headerBytes)
		if copied != sizeOfBasicV4EventHeader { // should never happen
			panic("Failed to copy header")
		}

		r.bodyBuffer = bufio2.NewLookAheadBufferUsing(
			r.src,
			r.nextEvent.data[sizeOfBasicV4EventHeader:])
	}

	_, err := r.bodyBuffer.PeekAll()
	if err != nil {
		return nil, err
	}

	// consume the constructed event and clean the look ahead buffers
	event := r.nextEvent
	r.nextEvent = nil
	r.headerBuffer = nil
	r.bodyBuffer = nil

	r.logPosition += int64(event.EventLength())

	return event, nil
}
