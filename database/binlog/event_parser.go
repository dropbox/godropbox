package binlog

import (
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type TableContext interface {
	// TableId returns which table the context is referring to.
	TableId() uint64

	// TableFlags returns the table's flags.
	TableFlags() uint16

	// DatabaseName returns which database the table belongs to.
	DatabaseName() []byte

	// TableName returns the table's name.
	TableName() []byte

	// NumColumns returns the number of columns in the table.
	NumColumns() int

	// ColumnDescriptors returns the columns' field descriptors.
	ColumnDescriptors() []ColumnDescriptor
}

// V4EventParser is the common parser interface for all v4 binlog event types.
type V4EventParser interface {
	// EventType returns the type of event that this parser can handle.
	EventType() mysql_proto.LogEventType_Type

	// FixedLengthDataSize returns the event's fixed-length data size.
	FixedLengthDataSize() int

	// SetTableContext provides context for rows events parsing.
	SetTableContext(context TableContext)

	// Parse processes a raw event's data bytes into a more useful
	// representation.  NOTE: the parser may assume the event's extra headers
	// size, fixed-length data and checksum size are coreectly set.  Also, it
	// may assume the raw event is of the correct event type.  When an error
	// occurs, the parser should return the original raw event along with the
	// error.
	Parse(raw *RawV4Event) (Event, error)
}

// V4EventParserMap holds a set of V4EventParsers.
type V4EventParserMap interface {
	// ExtraHeadersSize returns extra headers size for all events that are not
	// format description events (FDE's extra headers size is always 0)
	ExtraHeadersSize() int

	// Get returns the parser for the specified event type.
	Get(t mysql_proto.LogEventType_Type) V4EventParser

	// ChecksumSize returns the checksum's size for all events that are not
	// format description events (FDE's checksum size is always 0 for
	// mysql 5.5, 4 for mysql 5.6).
	ChecksumSize() int

	// SetChecksumSize is used for specifying the non-FDE events' checksum size.
	SetChecksumSize(size int)

	// SetTableContext sets the table map context for all registered
	// parsers.
	SetTableContext(context TableContext)

	// SetNumSupportedEventTypes sets the number of supported event types.
	// Calls to Get will return nil when the event type is larger than this
	// upper bound.
	SetNumSupportedEventTypes(num int)
}

// NOTE: the extra headers is empty as of mysql 5.6
// http://dev.mysql.com/doc/internals/en/event-structure.html
//
// TODO(patrick): extra headers is a poorly designed feature.  Revisit this
// if the length changes in the future.
const nonFDEExtraHeadersSize = 0

type v4EventParserMap struct {
	extraHeadersSize       int
	checksumSize           int
	numSupportedEventTypes int

	// TODO(patrick): maybe switch from map to array if lookup shows up on profile
	parsers map[mysql_proto.LogEventType_Type]V4EventParser
}

// NewV4EventParserMap returns an initialize V4EventParserMap with all handled
// event types' parsers registered.
func NewV4EventParserMap() V4EventParserMap {
	m := &v4EventParserMap{
		extraHeadersSize: nonFDEExtraHeadersSize,
		checksumSize:     0,
		parsers:          make(map[mysql_proto.LogEventType_Type]V4EventParser),
	}

	// TODO(patrick): implement parsers
	m.set(&FormatDescriptionEventParser{})
	m.set(&QueryEventParser{})
	m.set(&RotateEventParser{})
	m.set(&TableMapEventParser{})
	m.set(&XidEventParser{})
	m.set(&RowsQueryEventParser{})
	m.set(&GtidLogEventParser{})
	m.set(&PreviousGtidsLogEventParser{})

	m.set(newWriteRowsEventV1Parser())
	m.set(newWriteRowsEventV2Parser())
	m.set(newUpdateRowsEventV1Parser())
	m.set(newUpdateRowsEventV2Parser())
	m.set(newDeleteRowsEventV1Parser())
	m.set(newDeleteRowsEventV2Parser())
	m.set(newStopEventParser())

	m.numSupportedEventTypes = len(mysql_proto.LogEventType_Type_name)
	return m
}

func (m *v4EventParserMap) set(p V4EventParser) {
	if _, inMap := m.parsers[p.EventType()]; inMap {
		panic("Registering multiple parsers for event type: " +
			p.EventType().String())
	}
	m.parsers[p.EventType()] = p
}

func (m *v4EventParserMap) ExtraHeadersSize() int {
	return m.extraHeadersSize
}

func (m *v4EventParserMap) Get(t mysql_proto.LogEventType_Type) V4EventParser {
	if int(t) >= m.numSupportedEventTypes {
		return nil
	}
	return m.parsers[t]
}

func (m *v4EventParserMap) ChecksumSize() int {
	return m.checksumSize
}

func (m *v4EventParserMap) SetChecksumSize(size int) {
	m.checksumSize = size
}

func (m *v4EventParserMap) SetTableContext(context TableContext) {
	for _, p := range m.parsers {
		p.SetTableContext(context)
	}
}

func (m *v4EventParserMap) SetNumSupportedEventTypes(num int) {
	m.numSupportedEventTypes = num
}

type hasNoTableContext struct {
}

func (*hasNoTableContext) SetTableContext(context TableContext) {
	// do nothing
}

// No-op v4 event parser.  Useful for setting the fixed data length size.
type noOpV4EventParser struct {
	hasNoTableContext

	eventType           mysql_proto.LogEventType_Type
	fixedLengthDataSize int
}

// NewNoOpV4EventParser returns are parser which does nothing (except setting
// the fixed length data size when used in conjunction with
// ParsedV4EventReader).  This is mainly used for testing and FDE validation.
func NewNoOpV4EventParser(
	eventType mysql_proto.LogEventType_Type,
	fixedLengthDataSize int) V4EventParser {

	return &noOpV4EventParser{
		eventType:           eventType,
		fixedLengthDataSize: fixedLengthDataSize,
	}
}

func (p *noOpV4EventParser) EventType() mysql_proto.LogEventType_Type {
	return p.eventType
}

func (p *noOpV4EventParser) FixedLengthDataSize() int {
	return p.fixedLengthDataSize
}

func (p *noOpV4EventParser) Parse(raw *RawV4Event) (Event, error) {
	return raw, nil
}
