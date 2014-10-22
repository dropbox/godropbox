package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of the rows-query event.  NOTE: Rows-query event is not
// available in 5.5.
//
//  Rows-query event's payload is structured as follow
//      19 bytes for common v4 event header.
//      1 byte for the (truncated) query length (ignore this since there's a
//          bug in mysql's write_str_at_most_255_bytes).
//      the remaining is for the (maybe truncated) query (not zero terminated).
//      (optional) 4 bytes footer for checksum.
type RowsQueryEvent struct {
	Event

	truncatedQuery []byte
}

// This returns the (possibly truncated) query string.
func (e *RowsQueryEvent) TruncatedQuery() []byte {
	return e.truncatedQuery
}

//
// RowsQueryEventParser ------------------------------------------------------
//

type RowsQueryEventParser struct {
	hasNoTableContext
}

// RowsQueryEventParser's EventType always returns
// mysql_proto.LogEventType_ROWS_QUERY_EVENT.
func (p *RowsQueryEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_ROWS_QUERY_LOG_EVENT
}

// RowsQueryEventParser's FixedLengthDataSize always returns 0.
func (p *RowsQueryEventParser) FixedLengthDataSize() int {
	return 0
}

// RowsQueryEventParser's Parse processes a raw query event into a
// RowsQueryEvent.
func (p *RowsQueryEventParser) Parse(raw *RawV4Event) (Event, error) {
	query := &RowsQueryEvent{
		Event: raw,
	}

	data := raw.VariableLengthData()
	if len(data) < 1 {
		return raw, errors.Newf("Invalid message length")
	}

	query.truncatedQuery = data[1:]

	return query, nil
}
