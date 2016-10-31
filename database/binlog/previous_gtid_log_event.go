package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of the Previous GTID log event.
//
// Previous GTID log event's binlog payload is structured as follows:
//
//  5.6 Specific:
//      19 bytes for common v4 event header
//      8 byte unsigned n_sids
//      repeat n_sids times
//        16 bytes of sid
//        8 byte unsigned n_intervals
//        repeat n_intervals times
//          8 byte signed start
//          8 byte signed end

type PreviousGtidsLogEvent struct {
	Event

	set GtidSet
}

type GtidSet map[string][]GtidRange

type GtidRange struct {
	Start, End uint64 // NOTE: End is EXCLUSIVE
}

func (p *PreviousGtidsLogEvent) GtidSet() GtidSet {
	return p.set
}

type PreviousGtidsLogEventParser struct {
	hasNoTableContext
}

// PreviousGtidLogEventParser's EventType always returns
// mysql_proto.LogEventType_PREVIOUS_GTIDS_LOG_EVENT
func (p *PreviousGtidsLogEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_PREVIOUS_GTIDS_LOG_EVENT
}

// PreviousGtidLogEventParser's FixedLengthDataSize always returns 0.
func (p *PreviousGtidsLogEventParser) FixedLengthDataSize() int {
	return 0
}

// PreviousGtidLogEventParser's Parse processes a raw gtid log event into a PreviousGtidLogEvent.
func (p *PreviousGtidsLogEventParser) Parse(raw *RawV4Event) (Event, error) {
	pgle := &PreviousGtidsLogEvent{
		Event: raw,
		set:   make(map[string][]GtidRange),
	}

	data := raw.VariableLengthData()
	if len(data) < 8 {
		return nil, errors.Newf("Not enough bytes for n_sids: %v", data)
	}

	nSids, data := LittleEndian.Uint64(data[:8]), data[8:]
	for i := uint64(0); i < nSids; i++ {
		if len(data) < 16 {
			return nil, errors.Newf("Not enough bytes for sid: %v", data)
		}

		var sid string
		sid, data = string(data[:16]), data[16:]
		if len(data) < 8 {
			return nil, errors.Newf("Not enough bytes for n_intervals: %v", data)
		}

		var nIntervals uint64
		nIntervals, data = LittleEndian.Uint64(data[:8]), data[8:]
		for j := uint64(0); j < nIntervals; j++ {
			if len(data) < 16 {
				return nil, errors.Newf("Not enough bytes for start/end: %v", data)
			}

			var start, end uint64
			start, data = LittleEndian.Uint64(data[:8]), data[8:]
			end, data = LittleEndian.Uint64(data[:8]), data[8:]

			pgle.set[sid] = append(pgle.set[sid], GtidRange{start, end})
		}
	}

	if len(data) > 0 {
		return nil, errors.Newf("Extra bytes at the end: %v", data)
	}

	return pgle, nil
}
