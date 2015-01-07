package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// Event is the common interface for all mysql v4 binlog format events.
type Event interface {
	// SourceName returns the name of the event's source stream.
	SourceName() string

	// SourcePosition returns the position relative to the beginning of the
	// source stream.  Unlike the next position stored in the event header,
	// this position is always correct.
	SourcePosition() int64

	// Timestamp returns the event's timestamp.
	Timestamp() uint32

	// EventType returns the event's type.
	EventType() mysql_proto.LogEventType_Type

	// ServerId returns the id of the server which generated this event.
	ServerId() uint32

	// EventLength returns the event's total (header + body + footer) length.
	EventLength() uint32

	// NextPosition returns the next position stored in the event entry.
	// NOTE: This value is independent of the entry real position within the
	// source stream.  Use SourcePosition() to get the correct absolute
	// position relative to the beginning of the source stream.
	NextPosition() uint32

	// Flags returns the event's flags.
	Flags() uint16

	// Bytes returns the event payload (header + data)
	Bytes() []byte

	// BasicHeader returns the fixed length portion of the header bytes.
	BasicHeader() []byte

	// ExtraHeaders returns the extra header bytes associated to the event.
	ExtraHeaders() []byte

	// FixedLengthData returns the fixed length data associated to the event.
	// NOTE: in mysql source code, the length of this data is referred to as
	// the event's header length (i.e., FORMAT_DESCRIPTION_HEADER_LEN)
	FixedLengthData() []byte

	// VariableLengthData returns the variable length data associated to the
	// event.
	VariableLengthData() []byte

	// Checksum returns the checksum bytes (which may be empty).  NOTE:
	// checksum is an optional field introduced in 5.6.  The checksum
	// algorithm used is defined in the format description event.
	Checksum() []byte
}

const sizeOfBasicV4EventHeader = 19 // sizeof(basicV4EventHeader)

// Fixed-length portion of the v4 event header as described in
// http://dev.mysql.com/doc/internals/en/event-structure.html
type basicV4EventHeader struct {
	Timestamp    uint32
	EventType    uint8
	ServerId     uint32
	EventLength  uint32
	NextPosition uint32
	Flags        uint16
}

// A generic v4 Event entry.  This event is event type agnostic, i.e., the data
// (including the extra headers) is not interpreted.
type RawV4Event struct {
	sourceName     string
	sourcePosition int64

	header basicV4EventHeader

	extraHeadersSize    int
	fixedLengthDataSize int
	checksumSize        int
	data                []byte
}

// SourceName returns the name of the event's source stream.
func (e *RawV4Event) SourceName() string {
	return e.sourceName
}

// SourcePosition returns the position relative to the beginning of the source
// stream.  Unlike the next position stored in the event header, this position
// is always correct.
func (e *RawV4Event) SourcePosition() int64 {
	return e.sourcePosition
}

// Timestamp returns the event's timestamp.
func (e *RawV4Event) Timestamp() uint32 {
	return e.header.Timestamp
}

// EventType returns the event's type.
func (e *RawV4Event) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_Type(e.header.EventType)
}

// ServerId returns the id of the server which generated this event.
func (e *RawV4Event) ServerId() uint32 {
	return e.header.ServerId
}

// EventLength returns the event's total (header + body) length.
func (e *RawV4Event) EventLength() uint32 {
	return e.header.EventLength
}

// NextPosition returns the next position stored in the event entry.  NOTE:
// This value is independent of the entry real position within the source
// stream.  Use SourcePosition() to get the correct absolute position relative
// to the beginning of the source stream.
func (e *RawV4Event) NextPosition() uint32 {
	return e.header.NextPosition
}

// Flags returns the event's flags.
func (e *RawV4Event) Flags() uint16 {
	return e.header.Flags
}

// Bytes returns the event payload (header + data)
func (e *RawV4Event) Bytes() []byte {
	return e.data
}

// BasicHeader returns the fixed length portion of the header bytes.
func (e *RawV4Event) BasicHeader() []byte {
	return e.data[:sizeOfBasicV4EventHeader]
}

// RawV4Event's ExtraHeaders returns the extra header bytes associated to the
// event.  NOTE: by default, the extra header length is not set and this returns
// an empty byte slice.  Use SetExtraHeaderSize to specify the length.
func (e *RawV4Event) ExtraHeaders() []byte {
	begin := sizeOfBasicV4EventHeader
	end := sizeOfBasicV4EventHeader + e.extraHeadersSize
	return e.data[begin:end]
}

// RawV4Event's FixedLengthData returns the fixed legnth data associated to the
// event.  NOTE: by default, the fixed-length data's size is not set and this
// returns an empty byte slice.  Use SetFixedLengthDataSize to specify the
// length.
func (e *RawV4Event) FixedLengthData() []byte {
	begin := sizeOfBasicV4EventHeader + e.extraHeadersSize
	end := sizeOfBasicV4EventHeader + e.extraHeadersSize + e.fixedLengthDataSize
	return e.data[begin:end]
}

// VariableLengthData returns the variable length data associated to the event.
// By default, the variable length data also include the extra headers, the
// fixed length data and the optional checksum footer.
func (e *RawV4Event) VariableLengthData() []byte {
	begin := (sizeOfBasicV4EventHeader +
		e.extraHeadersSize +
		e.fixedLengthDataSize)
	end := len(e.data) - e.checksumSize
	return e.data[begin:end]
}

// Checksum returns the checksum bytes (which may be empty).  NOTE: by default,
// the checksum length is not set and this returns an empty byte slice.
// Use SetChecksumSize to specify the length.
func (e *RawV4Event) Checksum() []byte {
	return e.data[len(e.data)-e.checksumSize:]
}

// Set the extra headers' size.
func (e *RawV4Event) SetExtraHeadersSize(size int) error {
	newFixedSize := (size +
		sizeOfBasicV4EventHeader +
		e.fixedLengthDataSize +
		e.checksumSize)
	if size < 0 || newFixedSize > len(e.data) {
		return errors.Newf(
			"Invalid extra headers size (data size: %d basic header size: %d"+
				"fixed length data size: %d checksum size: %d input: %d)",
			len(e.data),
			sizeOfBasicV4EventHeader,
			e.fixedLengthDataSize,
			e.checksumSize,
			size)
	}
	e.extraHeadersSize = size
	return nil
}

// Set the fixed length data's size.
func (e *RawV4Event) SetFixedLengthDataSize(size int) error {
	newFixedSize := (size +
		sizeOfBasicV4EventHeader +
		e.extraHeadersSize +
		e.checksumSize)
	if size < 0 || newFixedSize > len(e.data) {
		return errors.Newf(
			"Invalid fixed length data's size (data size: %d "+
				"basic header size: %d extra headers size: %d "+
				"checksum size: %d input: %d)",
			len(e.data),
			sizeOfBasicV4EventHeader,
			e.extraHeadersSize,
			e.checksumSize,
			size)
	}
	e.fixedLengthDataSize = size
	return nil
}

// Set the checksum's size.
func (e *RawV4Event) SetChecksumSize(size int) error {
	newFixedSize := size + e.extraHeadersSize + e.fixedLengthDataSize
	if size < 0 || newFixedSize > len(e.data) {
		return errors.Newf(
			"Invalid checksum's size (data size: %d basic header size: %d"+
				"extra headers size: %d fixed length data size: %d input: %d)",
			len(e.data),
			sizeOfBasicV4EventHeader,
			e.extraHeadersSize,
			e.fixedLengthDataSize,
			size)
	}
	e.checksumSize = size
	return nil
}
