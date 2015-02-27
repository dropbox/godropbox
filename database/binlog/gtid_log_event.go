package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of the GTID log event.
//
// GTID log event's binlog payload is structured as follows:
//
//  5.6 Specific:
//      19 bytes for common v4 event header
//      1 byte for commit flag (1 or 0)
//      16 bytes for SID (server UUID)
//      8 bytes for GNO (transaction number) (stored in the binlog as an int64 but read from the binlog as a uint64?)

type GtidLogEvent struct {
	Event

	commit bool
	sid    [16]byte
	gno    uint64
}

func (e *GtidLogEvent) IsCommit() bool {
	return e.commit
}

func (e *GtidLogEvent) Sid() []byte {
	return e.sid[:]
}

func (e *GtidLogEvent) Gno() uint64 {
	return e.gno
}

type GtidLogEventParser struct {
	hasNoTableContext
}

// GtidLogEventParser's EventType always returns mysql_proto.LogEventType_GTID_LOG_EVENT
func (p *GtidLogEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_GTID_LOG_EVENT
}

// GtidLogEventParser's FixedLengthDataSize always returns 25.
func (p *GtidLogEventParser) FixedLengthDataSize() int {
	return 25
}

// GtidLogEventParser's Parse processes a raw gtid log event into a GtidLogEvent.
func (p *GtidLogEventParser) Parse(raw *RawV4Event) (Event, error) {
	gle := &GtidLogEvent{
		Event: raw,
	}

	if len(raw.VariableLengthData()) > 0 {
		return raw, errors.New("GTID binlog event larger than expected size")
	}

	data := raw.FixedLengthData()

	var commitData uint8
	data, err := readLittleEndian(data, &commitData)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read commit flag")
	}
	if commitData == 0 {
		gle.commit = false
	} else if commitData == 1 {
		gle.commit = true
	} else {
		return raw, errors.Newf("Commit data is not 0 or 1: %d", commitData)
	}

	data, err = readLittleEndian(data, &gle.sid)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read sid")
	}

	data, err = readLittleEndian(data, &gle.gno)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read GNO")
	}

	return gle, nil
}
