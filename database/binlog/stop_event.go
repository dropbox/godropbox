package binlog

import (
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of an event generated when mysqld stops.
//
// Common to both 5.5 and 5.6:
//      The Post-Header and Body of the event are empty.
//      The StopEvent only contains the Common-Header.
// 5.6 Specific:
//      (optional) 4 byte footer for checksum
type StopEvent struct {
	Event
}

//
// StopEventParser ------------------------------------------------------------
//

type stopEventParser struct {
	V4EventParser
}

func newStopEventParser() V4EventParser {
	return &stopEventParser{}
}

func (p *stopEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_STOP_EVENT
}

func (p *stopEventParser) FixedLengthDataSize() int {
	return 0
}

func (p *stopEventParser) SetTableContext(context TableContext) {
}

func (p *stopEventParser) Parse(raw *RawV4Event) (Event, error) {
	event := &StopEvent{
		Event: raw,
	}
	return event, nil
}
