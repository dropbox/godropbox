package binlog

import (
	"bytes"

	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of the format description event.
//
// FDE binlog payload is structured as follow:
//
//  Common to both 5.5 and 5.6:
//      19 bytes for common v4 event headers
//      2 bytes (uint16) for binlog version
//      50 bytes for server version string (padded with '\0's)
//      4 bytes (uint32) for created timestamp.  Note that this value may be
//          unpopulated.
//      1 byte (uint8) for total header size, where total header size =
//          common header size + extra headers size
//      1 byte per event type for event's fixed length data size.  Note that
//          unknown events does not have an entry.
//  5.5 Specific:
//      27 bytes for events' fixed size length (one uint8 entry per event
//          type, except unknown events)
//  5.6 Specific:
//      35 bytes for events' fixed size length (one uint8 entry per event
//          type, except unknown events)
//      1 byte (uint8) for checksum algorithm
//      4 bytes for checksum
type FormatDescriptionEvent struct {
	Event

	binlogVersion     uint16
	serverVersion     []byte // always 50 bytes
	createdTimestamp  uint32 // time_t ; may not be set
	extraHeadersSize  int
	fixedLengthSizes  map[mysql_proto.LogEventType_Type]int
	checksumAlgorithm mysql_proto.ChecksumAlgorithm_Type
}

// BinlogVersion returns the binlog version (which should always be 4)
func (e *FormatDescriptionEvent) BinlogVersion() uint16 {
	return e.binlogVersion
}

// ServerVersion returns the server version from which the events were emitted.
func (e *FormatDescriptionEvent) ServerVersion() []byte {
	return e.serverVersion
}

// CreatedTimestamp returns the fde's creation timestamp.  NOTE: mysql log
// writer may leave the timestamp undefined.
func (e *FormatDescriptionEvent) CreatedTimestamp() uint32 {
	return e.createdTimestamp
}

// ExtraHeaderSize returns the extra header size for non-FDE events.  For both
// mysql 5.5 and mysql 5.6, this should be 0.
func (e *FormatDescriptionEvent) ExtraHeadersSize() int {
	return e.extraHeadersSize
}

// NumKnownEventTypes returns the number of event types that is potentially in
// the stream.
func (e *FormatDescriptionEvent) NumKnownEventTypes() int {
	return len(e.fixedLengthSizes)
}

// FixedLengthDataSizeForType returns the size of fixed length data for each
// event type.
func (e *FormatDescriptionEvent) FixedLengthDataSizeForType(
	eventType mysql_proto.LogEventType_Type) int {

	return e.fixedLengthSizes[eventType]
}

// ChecksumAlgorithm returns the algorithm used for checksumming non-FDE events
func (e *FormatDescriptionEvent) ChecksumAlgorithm() mysql_proto.ChecksumAlgorithm_Type {

	return e.checksumAlgorithm
}

//
// FormatDescriptionEventParser -----------------------------------------------
//

const (
	FDEFixedLengthDataSizeFor55 = 2 + 50 + 4 + 1 + 27
	FDEFixedLengthDataSizeFor56 = 2 + 50 + 4 + 1 + 35
)

type FormatDescriptionEventParser struct {
	hasNoTableContext
}

// FormatDescriptionEventParser's EventType always returns
// mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT.
func (p *FormatDescriptionEventParser) EventType() mysql_proto.LogEventType_Type {

	return mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT
}

// FormatDescriptionEventParser's FixedLengthDataSize always returns 0 (i.e.,
// we pretend FDE does not have fixed length data).  NOTE: In 5.6, the real
// "fixed" number is FDEFixedLengthDataSizeFor56.  In 5.5, the real "fixed"
// number is FDEFixedLengthDataSizeFor55.  The difference is due to increased
// number of event types.
func (p *FormatDescriptionEventParser) FixedLengthDataSize() int {
	return 0
}

// FormatDecriptionEventParser's Parse processes a raw FDE event into a
// FormatDescriptionEvent.
func (p *FormatDescriptionEventParser) Parse(raw *RawV4Event) (Event, error) {
	fde := &FormatDescriptionEvent{
		Event:            raw,
		fixedLengthSizes: make(map[mysql_proto.LogEventType_Type]int),
	}

	data := raw.VariableLengthData()

	data, err := readLittleEndian(data, &fde.binlogVersion)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read binlog version")
	}

	serverVersion, data, err := readSlice(data, 50)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read server version")
	}
	if idx := bytes.IndexByte(serverVersion, byte(0)); idx > -1 {
		serverVersion = serverVersion[:idx]
	}
	fde.serverVersion = serverVersion

	data, err = readLittleEndian(data, &fde.createdTimestamp)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read created timestamp")
	}

	var totalHeaderSize uint8
	data, err = readLittleEndian(data, &totalHeaderSize)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read total header size")
	}
	fde.extraHeadersSize = int(totalHeaderSize) - sizeOfBasicV4EventHeader

	numEvents := len(mysql_proto.LogEventType_Type_value)
	hasChecksum := true

	if len(data) == 27 { // mysql 5.5(.37)
		numEvents = 28
		hasChecksum = false
	} else if len(data) == 40 { // mysql 5.6(.17)

		// This is a relay log where the master is 5.5 and slave is 5.6
		if data[int(mysql_proto.LogEventType_WRITE_ROWS_EVENT)-1] == 0 {
			numEvents = 28
		}
	} else {
		return raw, errors.Newf(
			"Unable to parse FDE for mysql variant: %s",
			fde.serverVersion)
	}

	// unknown event's fixed length is implicit.
	fde.fixedLengthSizes[mysql_proto.LogEventType_UNKNOWN_EVENT] = 0
	for i := 1; i < numEvents; i++ {
		fde.fixedLengthSizes[mysql_proto.LogEventType_Type(i)] = int(data[i-1])
	}

	if hasChecksum {
		fde.checksumAlgorithm = mysql_proto.ChecksumAlgorithm_Type(
			data[len(data)-5])

		raw.SetChecksumSize(4)
	}

	return fde, nil
}
