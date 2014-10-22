package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of the xid event.
//
// Xid event's binlog payload is structured as follow:
//
//  Common to both 5.5 and 5.6:
//      19 bytes for common v4 event headers
//      8 bytes (uint64) for xid.  NOTE: xid is written using the master's
//          machine endianness.  The id's value will differ when read on
//          different processor platforms; however, replication will function
//          correctly since the uniqueness of the id is preserved.
//  5.6 Specific:
//      (optional) 4 bytes footer for checksum
type XidEvent struct {
	Event

	xid uint64
}

// Xid returns the event's transaction id.
func (e *XidEvent) Xid() uint64 {
	return e.xid
}

//
// XidEventParser -------------------------------------------------------------
//

type XidEventParser struct {
	hasNoTableContext
}

// XidEventParser's EventType always returns mysql_proto.LogEventType_XID_EVENT
func (p *XidEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_XID_EVENT
}

// XidEventParser's FixedLengthDataSize always return 0.
func (p *XidEventParser) FixedLengthDataSize() int {
	return 0
}

// XidEventParser's Parse processes a raw xid event into a XidEvent.
func (p *XidEventParser) Parse(raw *RawV4Event) (Event, error) {
	xe := &XidEvent{
		Event: raw,
	}

	// For convenience, we'll interpret the bytes as little endian, our
	// dominate computing (intel) platform.
	_, err := readLittleEndian(raw.VariableLengthData(), &xe.xid)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read xid")
	}

	return xe, nil
}
