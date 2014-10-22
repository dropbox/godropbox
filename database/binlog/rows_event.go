package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// BaseRowsEvent is the representation common to all v1/v2
// write/update/delete rows events
//
// TODO(patrick): figure out what's inside the extra_row_info bytes (so far,
// my searches lead to dead ends; looks almost like it's not used.)
//
//  Common to both 5.5 and 5.6:
//      19 bytes for common v4 event header
//      6 bytes (uint64) for table id
//      2 bytes (uint16) for flags
//  V2 rows events specific (5.6 only):
//      2 bytes (uint16), X, for 2 + the length of variable-sized header
//      X bytes for variable-sized header:
//          (optional) extra info tag:
//              1 byte for RW_V_EXTRAINFO_TAG (=0)
//              1 byte (uint8), Y, for extra row info length
//              Y bytes for extra row info data
//  Common to both 5.5 and 5.6:
//      1 to 9 bytes (net_store_length variable encoded uint64), Z, for total
//              number of columns (XXX: should be same as # of columns in table
//              map event?).
//      ceil(Z / 8) bytes for bitmap indicating which columns are used.  (NOTE:
//              for update events, this bitmap is used for the before image).
//  V1/v2 update events specific:
//      ceil(Z / 8) bytes for bitmap indicating which columns are used in the
//              after image.
//  Common to both 5.5 and 5.6:
//      The remaining body contains the row data (row values are decoded based
//              on current table context):
//          V1/v2 write/delete events specific:
//              List of rows
//          V1/v2 update events specific:
//              List of pairs of (before image row, after image row)
//          Each row image is compose of:
//              bit field indicating whether each field in the row is NULL.
//              list of non-NULL encoded values.
//  5.6 Specific:
//      (optional) 4 bytes footer for checksum
type BaseRowsEvent struct {
	Event

	version mysql_proto.RowsEventVersion_Type

	context TableContext

	tableId    uint64
	rowsFlags  uint16
	numColumns int

	extraRowInfoBytes []byte // nil for v1 events
	rowDataBytes      []byte // this does not include the used columns bit map.
}

// Version returns the event's encoding version.
func (e *BaseRowsEvent) Version() mysql_proto.RowsEventVersion_Type {
	return e.version
}

// Context returns the event's table context.
func (e *BaseRowsEvent) Context() TableContext {
	return e.context
}

// TableId returns which table the event should act on.
func (e *BaseRowsEvent) TableId() uint64 {
	return e.tableId
}

// RowsFlags returns the rows event's flags.
func (e *BaseRowsEvent) RowsFlags() uint16 {
	return e.rowsFlags
}

// NumColumns returns the table's width.
func (e *BaseRowsEvent) NumColumns() int {
	return e.numColumns
}

// ExtraRowInfoBytes returns the uninterpreted extra row info bytes.  NOTE:
// When the event's encoding version is v1, this always returns nil.
func (e *BaseRowsEvent) ExtraRowInfoBytes() []byte {
	return e.extraRowInfoBytes
}

// RowDataBytes returns the uninterpreted row data bytes.  NOTE: This does
// not include the used columns bit map.
func (e *BaseRowsEvent) RowDataBytes() []byte {
	return e.rowDataBytes
}

// A single row's used columns values.
type RowValues []interface{}

// A representation of the v1 / v2 write rows event.
type WriteRowsEvent struct {
	BaseRowsEvent

	usedColumns []ColumnDescriptor

	rows []RowValues
}

// UsedColumns returns the column descriptors that are used by the event.
func (e *WriteRowsEvent) UsedColumns() []ColumnDescriptor {
	return e.usedColumns
}

// InsertedRows returns the rows written into the table.
func (e *WriteRowsEvent) InsertedRows() []RowValues {
	return e.rows
}

// A representation of the v1 / v2 delete rows event.
type DeleteRowsEvent struct {
	BaseRowsEvent

	usedColumns []ColumnDescriptor

	rows []RowValues
}

// UsedColumns returns the column descriptors that are used by the event.
func (e *DeleteRowsEvent) UsedColumns() []ColumnDescriptor {
	return e.usedColumns
}

// DeletedRows returns the rows removed from the table.
func (e *DeleteRowsEvent) DeletedRows() []RowValues {
	return e.rows
}

// A single update row's used columns values.
type UpdateRowValues struct {
	BeforeImage RowValues
	AfterImage  RowValues
}

// A representation of the v1 / v2 update rows event.
type UpdateRowsEvent struct {
	BaseRowsEvent

	beforeImageUsedColumns []ColumnDescriptor
	afterImageUsedColumns  []ColumnDescriptor

	rows []UpdateRowValues
}

// BeforeImageUsedColumns returns the before image column descriptors that
// are used by the event.
func (e *UpdateRowsEvent) BeforeImageUsedColumns() []ColumnDescriptor {
	return e.beforeImageUsedColumns
}

// AfterImageUsedColumns returns the after image column descriptors that
// are used by the event.
func (e *UpdateRowsEvent) AfterImageUsedColumns() []ColumnDescriptor {
	return e.afterImageUsedColumns
}

// UpdatedRows returns the rows in the table that were mutated.
func (e *UpdateRowsEvent) UpdatedRows() []UpdateRowValues {
	return e.rows
}

//
// baseRowsEventParser --------------------------------------------------------
//

// This expected error can occur when reading from a new log file.
type TableContextNotSetError struct {
	errors.DropboxError
}

// Parse is left unimplemented.
type baseRowsEventParser struct {
	eventType mysql_proto.LogEventType_Type

	version mysql_proto.RowsEventVersion_Type

	context TableContext
}

func (p *baseRowsEventParser) EventType() mysql_proto.LogEventType_Type {
	return p.eventType
}

func (p *baseRowsEventParser) FixedLengthDataSize() int {
	if p.version == mysql_proto.RowsEventVersion_V1 {
		return 8
	}
	return 10 // V2
}

func (p *baseRowsEventParser) SetTableContext(context TableContext) {
	p.context = context
}

func (p *baseRowsEventParser) parseRowsHeader(raw *RawV4Event) (
	id uint64,
	flags uint16,
	extraInfo []byte,
	width int,
	remaining []byte,
	err error) {

	if p.context == nil {
		return 0, 0, nil, 0, nil, &TableContextNotSetError{
			errors.New("Table context not set"),
		}
	}

	data := raw.FixedLengthData()

	id = LittleEndian.Uint48(data)

	if id != p.context.TableId() {
		return 0, 0, nil, 0, nil, errors.Newf(
			"mismatch table id (event: %d; context: %d name: %s)",
			id,
			p.context.TableId(),
			p.context.TableName())
	}

	flags = LittleEndian.Uint16(data[6:])

	remaining = raw.VariableLengthData()

	if p.version == mysql_proto.RowsEventVersion_V1 {
		extraInfo = nil
	} else {
		extraInfoBlobLen := LittleEndian.Uint16(data[8:])
		extraInfo, remaining, err = p.parseExtraInfoBlob(
			extraInfoBlobLen,
			remaining)
		if err != nil {
			return 0, 0, nil, 0, nil, err
		}
	}

	w, remaining, err := readFieldLength(remaining)
	if err != nil {
		return 0, 0, nil, 0, nil, err
	}

	if w > uint64(p.context.NumColumns()) {
		return 0, 0, nil, 0, nil, errors.Newf(
			"row width (%d / %d) greater than # of columns (%d) in table %s",
			w,
			width,
			p.context.NumColumns(),
			string(p.context.TableName()))
	}

	return id, flags, extraInfo, int(w), remaining, nil
}

const extraInfoTag = 0

func (p *baseRowsEventParser) parseExtraInfoBlob(
	extraInfoBlobLen uint16,
	data []byte) (
	extraInfo []byte,
	remaining []byte,
	err error) {

	// NOTE: the stored value includes the size of this length field.  Its
	// value should always be greater or equal to two.
	if extraInfoBlobLen < 2 {
		return nil, nil, errors.New("Invalid extra info length")
	}

	extraInfo = nil
	remaining = data

	extraInfoBlobLen -= 2
	if extraInfoBlobLen > 0 {
		if len(remaining) < 2 {
			return nil, nil, errors.New("Not enough bytes")
		}

		tag := uint8(remaining[0])
		extraInfoLen := uint8(remaining[1])
		remaining = remaining[2:]

		if tag != extraInfoTag {
			return nil, nil, errors.Newf("Unexpected tag: %d", remaining[0])
		}

		// For now, there's only one extra info type.  The blob length should
		// match the extra info length.
		if extraInfoBlobLen != uint16(extraInfoLen+2) {
			return nil, nil, errors.New("Invalid extra info")
		}

		extraInfo, remaining, err = readSlice(remaining, int(extraInfoLen))
		if err != nil {
			return nil, nil, err
		}
	}

	return extraInfo, remaining, nil
}

func (p *baseRowsEventParser) parseUsedColumns(
	width int,
	data []byte) (
	usedColumns []ColumnDescriptor,
	remaining []byte,
	err error) {

	usedColumnBits, remaining, err := readBitArray(data, width)
	if err != nil {
		return nil, nil, err
	}

	allColumns := p.context.ColumnDescriptors()

	usedColumns = make([]ColumnDescriptor, 0, 0)
	for idx := 0; idx < width; idx++ {
		if usedColumnBits[idx] {
			usedColumns = append(usedColumns, allColumns[idx])
		}
	}
	return usedColumns, remaining, nil
}

func (p *baseRowsEventParser) parseRow(
	usedColumns []ColumnDescriptor,
	data []byte) (
	row RowValues,
	remaining []byte,
	err error) {

	numCols := len(usedColumns)
	nullBits, remaining, err := readBitArray(data, numCols)
	if err != nil {
		return nil, nil, err
	}

	values := make(RowValues, numCols, numCols)
	for idx, descriptor := range usedColumns {
		if nullBits[idx] {
			if !descriptor.IsNullable() {
				return nil, nil, errors.Newf(
					"Null value in non-nullable column: %d table: %s",
					descriptor.IndexPosition(),
					string(p.context.TableName()))
			}
			values[idx] = nil
			continue
		}

		var val interface{}
		val, remaining, err = descriptor.ParseValue(remaining)
		if err != nil {
			return nil, nil, err
		}

		values[idx] = val
	}

	return values, remaining, nil
}

//
// WriteRowsEventParser -------------------------------------------------------
//

type WriteRowsEventParser struct {
	baseRowsEventParser
}

func newWriteRowsEventV1Parser() V4EventParser {
	return &WriteRowsEventParser{
		baseRowsEventParser: baseRowsEventParser{
			eventType: mysql_proto.LogEventType_WRITE_ROWS_EVENT_V1,
			version:   mysql_proto.RowsEventVersion_V1,
		},
	}
}

func newWriteRowsEventV2Parser() V4EventParser {
	return &WriteRowsEventParser{
		baseRowsEventParser: baseRowsEventParser{
			eventType: mysql_proto.LogEventType_WRITE_ROWS_EVENT,
			version:   mysql_proto.RowsEventVersion_V2,
		},
	}
}

func (p *WriteRowsEventParser) Parse(raw *RawV4Event) (Event, error) {
	id, flags, extraInfo, width, remaining, err := p.parseRowsHeader(raw)
	if err != nil {
		return raw, err
	}

	e := &WriteRowsEvent{
		BaseRowsEvent: BaseRowsEvent{
			Event:             raw,
			version:           p.version,
			context:           p.context,
			tableId:           id,
			rowsFlags:         flags,
			numColumns:        width,
			extraRowInfoBytes: extraInfo,
		},
		rows: make([]RowValues, 0, 0),
	}

	descriptors, remaining, err := p.parseUsedColumns(width, remaining)
	if err != nil {
		return raw, err
	}

	e.usedColumns = descriptors
	e.rowDataBytes = remaining

	for len(remaining) > 0 {
		var row RowValues
		row, remaining, err = p.parseRow(descriptors, remaining)
		if err != nil {
			return raw, err
		}
		e.rows = append(e.rows, row)
	}

	return e, nil
}

//
// UpdateRowsEventParser ------------------------------------------------------
//

type UpdateRowsEventParser struct {
	baseRowsEventParser
}

func newUpdateRowsEventV1Parser() V4EventParser {
	return &UpdateRowsEventParser{
		baseRowsEventParser: baseRowsEventParser{
			eventType: mysql_proto.LogEventType_UPDATE_ROWS_EVENT_V1,
			version:   mysql_proto.RowsEventVersion_V1,
		},
	}
}

func newUpdateRowsEventV2Parser() V4EventParser {
	return &UpdateRowsEventParser{
		baseRowsEventParser: baseRowsEventParser{
			eventType: mysql_proto.LogEventType_UPDATE_ROWS_EVENT,
			version:   mysql_proto.RowsEventVersion_V2,
		},
	}
}

func (p *UpdateRowsEventParser) Parse(raw *RawV4Event) (Event, error) {
	id, flags, extraInfo, width, remaining, err := p.parseRowsHeader(raw)
	if err != nil {
		return raw, err
	}

	e := &UpdateRowsEvent{
		BaseRowsEvent: BaseRowsEvent{
			Event:             raw,
			version:           p.version,
			context:           p.context,
			tableId:           id,
			rowsFlags:         flags,
			numColumns:        width,
			extraRowInfoBytes: extraInfo,
		},
		rows: make([]UpdateRowValues, 0, 0),
	}

	beforeDescriptors, remaining, err := p.parseUsedColumns(width, remaining)
	if err != nil {
		return raw, err
	}

	afterDescriptors, remaining, err := p.parseUsedColumns(width, remaining)
	if err != nil {
		return raw, err
	}

	e.beforeImageUsedColumns = beforeDescriptors
	e.afterImageUsedColumns = afterDescriptors
	e.rowDataBytes = remaining

	for len(remaining) > 0 {
		var beforeImage RowValues
		beforeImage, remaining, err = p.parseRow(
			beforeDescriptors,
			remaining)
		if err != nil {
			return raw, err
		}

		var afterImage RowValues
		afterImage, remaining, err = p.parseRow(
			afterDescriptors,
			remaining)
		if err != nil {
			return raw, err
		}
		e.rows = append(e.rows, UpdateRowValues{beforeImage, afterImage})
	}

	return e, nil
}

//
// DeleteRowsEventParser ------------------------------------------------------
//

type DeleteRowsEventParser struct {
	baseRowsEventParser
}

func newDeleteRowsEventV1Parser() V4EventParser {
	return &DeleteRowsEventParser{
		baseRowsEventParser: baseRowsEventParser{
			eventType: mysql_proto.LogEventType_DELETE_ROWS_EVENT_V1,
			version:   mysql_proto.RowsEventVersion_V1,
		},
	}
}

func newDeleteRowsEventV2Parser() V4EventParser {
	return &DeleteRowsEventParser{
		baseRowsEventParser: baseRowsEventParser{
			eventType: mysql_proto.LogEventType_DELETE_ROWS_EVENT,
			version:   mysql_proto.RowsEventVersion_V2,
		},
	}
}

func (p *DeleteRowsEventParser) Parse(raw *RawV4Event) (Event, error) {
	id, flags, extraInfo, width, remaining, err := p.parseRowsHeader(raw)
	if err != nil {
		return raw, err
	}

	e := &DeleteRowsEvent{
		BaseRowsEvent: BaseRowsEvent{
			Event:             raw,
			version:           p.version,
			context:           p.context,
			tableId:           id,
			rowsFlags:         flags,
			numColumns:        width,
			extraRowInfoBytes: extraInfo,
		},
		rows: make([]RowValues, 0, 0),
	}

	descriptors, remaining, err := p.parseUsedColumns(width, remaining)
	if err != nil {
		return raw, err
	}

	e.usedColumns = descriptors
	e.rowDataBytes = remaining

	for len(remaining) > 0 {
		var row RowValues
		row, remaining, err = p.parseRow(descriptors, remaining)
		if err != nil {
			return raw, err
		}
		e.rows = append(e.rows, row)
	}

	return e, nil
}
