package binlog

import (
	mysql_proto "dropbox/proto/mysql"
	"godropbox/errors"
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
//	5.7 Specific:
//		1 byte for lt_type
//		8 bytes for lastCommited
//		8 bytes for sequenceNumber
//

// The layout of the buffer in 5.7 is as follows:
// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/src/control_events.cpp#L626
// +------+--------+-------+-------+--------------+---------------+
// |flags |SID     |GNO    |lt_type|last_committed|sequence_number|
// |1 byte|16 bytes|8 bytes|1 byte |8 bytes       |8 bytes        |
// +------+--------+-------+-------+--------------+---------------+


const (
	GTIDLogEventFixedLengthDataSizeFor56 = 25
	GTIDLogEventFixedLengthDataSizeFor57 = 42
	logicalTimestampTypeCode uint8 = 2;
)

type GtidLogEvent struct {
	Event

	commit         bool
	sid            [16]byte
	gno            uint64
	lastCommited   int64
	sequenceNumber int64
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

// GtidLogEventParser's FixedLengthDataSize always returns 0 (i.e.,
// we pretend it does not have fixed length data).
//  NOTE:
// In 5.6, the real "fixed" number is GTIDLogEventFixedLengthDataSizeFor56.
// In 5.7, the real "fixed" number is GTIDLogEventFixedLengthDataSizeFor57.
// The difference is due to the new group commit logic
func (p *GtidLogEventParser) FixedLengthDataSize() int {
	return 0
}

// GtidLogEventParser's Parse processes a raw gtid log event into a GtidLogEvent.
func (p *GtidLogEventParser) Parse(raw *RawV4Event) (Event, error) {
	gle := &GtidLogEvent{
		Event: raw,
	}

	data := raw.VariableLengthData()

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
	if len(data) > 16  { // 5.7
		// https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h#L1045

		var timestampTypeCode uint8
		data, err = readLittleEndian(data, &timestampTypeCode)
		if err != nil {
			return raw, errors.Wrap(err, "Failed to read logicalTimestampTypeCode")
		}

		if timestampTypeCode == logicalTimestampTypeCode {
			data, err = readLittleEndian(data, &gle.lastCommited)
			if err != nil {
				return raw, errors.Wrap(err, "Failed to read lastCommited")
			}
			data, err = readLittleEndian(data, &gle.sequenceNumber)
			if err != nil {
				return raw, errors.Wrap(err, "Failed to read sequenceNumber")
			}
		}
	}
	if len(data) > 0 {
		return raw, errors.New("GTID binlog event larger than expected size")
	}

	return gle, nil
}
