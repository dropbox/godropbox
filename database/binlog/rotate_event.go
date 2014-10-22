package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of the rotate event.  NOTE: Users should ignore rotate
// events that originated from relay logs.
//
// Ratate event's binlog payload is structured as follow:
//
//  Common to both 5.5 and 5.6:
//      19 bytes for common v4 event headers
//      8 bytes (uint64) for offset position
//      the remaining for the new log name (not zero terminated).
//  5.6 Specific:
//      (optional) 4 bytes footer for checksum.
type RotateEvent struct {
	Event

	newLogName  []byte
	newPosition uint64
}

// NewLogName returns the name of the new log file to read from.
func (e *RotateEvent) NewLogName() []byte {
	return e.newLogName
}

// NewPosition returns the position in the new log file to seek to (In
// practice, this should always return 4).
func (e *RotateEvent) NewPosition() uint64 {
	return e.newPosition
}

//
// RotateEventParser ----------------------------------------------------------
//

type RotateEventParser struct {
	hasNoTableContext
}

// RotateEventParser's EventType always returns
// mysql_proto.LogEventType_ROTATE_EVENT.
func (p *RotateEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_ROTATE_EVENT
}

// RotateEventParser's FixedLengthDataSize always returns 8.
func (p *RotateEventParser) FixedLengthDataSize() int {
	return 8
}

// RotateEventParser's Parse processes a raw rotate event into a RotateEvent.
func (p *RotateEventParser) Parse(raw *RawV4Event) (Event, error) {
	rotate := &RotateEvent{
		Event:      raw,
		newLogName: raw.VariableLengthData(),
	}

	_, err := readLittleEndian(raw.FixedLengthData(), &rotate.newPosition)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read new log position")
	}

	return rotate, nil
}
