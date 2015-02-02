package binlog

import (
	"math"

	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// A representation of the table map event.
//
//  Common to both 5.5 and 5.6:
//      19 bytes for common v4 event header
//      6 bytes (uint64) for table id
//      2 bytes (uint16) for flags (as of 5.6, this is always 0)
//      1 byte (uint8), x, for db name length (WARNING: mysql assumes the db
//          name length is always less than 255; the log writer truncates
//          the db name length from size_t to uchar without checking)
//      x + 1 bytes for db name (zero-terminated)
//      1 byte (uint8), y, for table name length (WARNING: mysql assumes the
//          table name length is always less than 255; the log writer truncates
//          the table name length from size_t to uchar without checking)
//      y + 1 bytes for table name (zero-terminated)
//      1 to 9 bytes (net_store_length variable encoded uint64), z, for number
//          of columns
//      z bytes for column types (1 byte per column)
//      1 to 9 bytes (net_store_length variable encoded uint64), w, for
//          field metadata size
//      w bytes for field metadata
//      ceil(z / 8) bytes for nullable columns (1 bit per column)
//  5.6 Specific:
//      (optional) 4 bytes footer for checksum
//  NOTE:
//      - old_row_based_repl_4_byte_map_id_master mode is not supported.
type TableMapEvent struct {
	Event

	tableId      uint64
	tableFlags   uint16
	databaseName []byte
	tableName    []byte

	columnTypesBytes []byte
	metadataBytes    []byte
	nullColumnsBytes []byte

	columnDescriptors []ColumnDescriptor
}

// TableId returns which table the following row event entries should act on.
func (e *TableMapEvent) TableId() uint64 {
	return e.tableId
}

// TableFlags returns the table's flags.  (As of 5.6, this is always 0).
func (e *TableMapEvent) TableFlags() uint16 {
	return e.tableFlags
}

// DatabaseName returns which database the table belongs to.
func (e *TableMapEvent) DatabaseName() []byte {
	return e.databaseName
}

// TableName returns the table's name.
func (e *TableMapEvent) TableName() []byte {
	return e.tableName
}

// NumColumns returns the number of columns in the table.
func (e *TableMapEvent) NumColumns() int {
	return len(e.columnTypesBytes)
}

// ColumnTypesBytes returns the columns' types as uninterpreted bytes.
func (e *TableMapEvent) ColumnTypesBytes() []byte {
	return e.columnTypesBytes
}

// MetadataBytes returns the metadata associated to the columns as
// uninterpreted bytes.
func (e *TableMapEvent) MetadataBytes() []byte {
	return e.metadataBytes
}

// NullColumnsBytes returns the null column bit vector as uninterpreted bytes.
func (e *TableMapEvent) NullColumnsBytes() []byte {
	return e.nullColumnsBytes
}

// ColumnDescriptors returns the columns' field descriptors parsed from
// ColumnTypesBytes/MetadataBytes/NullColumnsBytes.
func (e *TableMapEvent) ColumnDescriptors() []ColumnDescriptor {
	return e.columnDescriptors
}

//
// TableMapEventParser --------------------------------------------------------
//

type TableMapEventParser struct {
	hasNoTableContext
}

// TableMapEventParser's EventType always returns
// mysql_proto.LogEventType_TABLE_MAP_EVENT.
func (p *TableMapEventParser) EventType() mysql_proto.LogEventType_Type {
	return mysql_proto.LogEventType_TABLE_MAP_EVENT
}

// TableMapEventParser's FixedLengthDataSize always returns 8.
func (p *TableMapEventParser) FixedLengthDataSize() int {
	return 8
}

// TableMapEventParser's Parse processes a raw table map event into
// TableMapEvent.
func (p *TableMapEventParser) Parse(raw *RawV4Event) (Event, error) {
	table := &TableMapEvent{
		Event: raw,
	}

	data := raw.FixedLengthData()

	table.tableId = LittleEndian.Uint48(data)

	_, err := readLittleEndian(data[6:], &table.tableFlags)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read table flags")
	}

	data = raw.VariableLengthData()

	var dbNameLength uint8
	data, err = readLittleEndian(data, &dbNameLength)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read db name length")
	}

	dbName, data, err := readSlice(data, int(dbNameLength)+1)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read db name")
	}
	table.databaseName = dbName[:dbNameLength] // drop zero terminate char

	var tableNameLength uint8
	data, err = readLittleEndian(data, &tableNameLength)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read table name length")
	}

	tableName, data, err := readSlice(data, int(tableNameLength)+1)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read table name")
	}
	table.tableName = tableName[:tableNameLength]

	numColumns, data, err := readFieldLength(data)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read number of columns")
	}
	if numColumns > math.MaxInt32 { // the event is probably corrupted
		return raw, errors.Wrap(err, "Too many columns")
	}

	table.columnTypesBytes, data, err = readSlice(data, int(numColumns))
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read column types")
	}

	metadataSize, data, err := readFieldLength(data)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read metadata size")
	}
	if metadataSize > math.MaxInt32 { // the event is probably corrupted
		return raw, errors.Wrap(err, "Too much metadata")
	}

	table.metadataBytes, data, err = readSlice(data, int(metadataSize))
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read metadata")
	}

	table.nullColumnsBytes, _, err = readSlice(data, int((numColumns+7)/8))
	if err != nil {
		return raw, errors.Wrap(err, "Failed to read null bit vector")
	}

	err = p.parseColumns(table)
	if err != nil {
		return raw, errors.Wrap(err, "Failed to parse column descriptions")
	}

	return table, nil
}

func (p *TableMapEventParser) parseColumns(t *TableMapEvent) error {
	numCols := len(t.columnTypesBytes)
	t.columnDescriptors = make([]ColumnDescriptor, numCols, numCols)

	metadata := t.metadataBytes

	nullVector, _, err := readBitArray(t.nullColumnsBytes, numCols)
	if err != nil {
		return err
	}

	for idx, colTypeByte := range t.columnTypesBytes {
		colType := mysql_proto.FieldType_Type(colTypeByte)
		realType := colType
		metaLength := 0
		if colType == mysql_proto.FieldType_STRING ||
			colType == mysql_proto.FieldType_VAR_STRING {

			realType, metaLength, metadata, err = parseTypeAndLength(metadata)
			if err != nil {
				return err
			}

			// mysql_proto.FieldType_VAR_STRING is not type polymorphic.
			if colType == mysql_proto.FieldType_VAR_STRING &&
				colType != realType {

				return errors.Newf("Invalid real type: %s (%d)",
					realType.String(),
					realType)
			}
		}

		var fd FieldDescriptor

		nullable := NullableColumn(nullVector[idx])

		switch realType {
		case mysql_proto.FieldType_DECIMAL:
			fd = NewDecimalFieldDescriptor(nullable)
		case mysql_proto.FieldType_TINY:
			fd = NewTinyFieldDescriptor(nullable)
		case mysql_proto.FieldType_SHORT:
			fd = NewShortFieldDescriptor(nullable)
		case mysql_proto.FieldType_LONG:
			fd = NewLongFieldDescriptor(nullable)
		case mysql_proto.FieldType_FLOAT:
			fd, metadata, err = NewFloatFieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_DOUBLE:
			fd, metadata, err = NewDoubleFieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_NULL:
			fd = NewNullFieldDescriptor(nullable)
		case mysql_proto.FieldType_TIMESTAMP:
			fd = NewTimestampFieldDescriptor(nullable)
		case mysql_proto.FieldType_LONGLONG:
			fd = NewLongLongFieldDescriptor(nullable)
		case mysql_proto.FieldType_INT24:
			fd = NewInt24FieldDescriptor(nullable)
		case mysql_proto.FieldType_DATE:
			return errors.New("TODO")
		case mysql_proto.FieldType_TIME:
			return errors.New("TODO")
		case mysql_proto.FieldType_DATETIME:
			fd = NewDateTimeFieldDescriptor(nullable)
		case mysql_proto.FieldType_YEAR:
			fd = NewYearFieldDescriptor(nullable)
		case mysql_proto.FieldType_NEWDATE:
			return errors.New("TODO")
		case mysql_proto.FieldType_VARCHAR:
			fd, metadata, err = NewVarcharFieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_BIT:
			fd, metadata, err = NewBitFieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_TIMESTAMP2:
			fd, metadata, err = NewTimestamp2FieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_DATETIME2:
			fd, metadata, err = NewDateTime2FieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_TIME2:
			return errors.New("TODO")
		case mysql_proto.FieldType_NEWDECIMAL:
			fd, metadata, err = NewNewDecimalFieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_ENUM:
			return errors.New("Enum type should not appear in binlog")
		case mysql_proto.FieldType_SET:
			return errors.New("Set type should not appear in binlog")
		case mysql_proto.FieldType_TINY_BLOB:
			return errors.New("Tiny blog type should not appear in binlog")
		case mysql_proto.FieldType_MEDIUM_BLOB:
			return errors.New("Medium blog type should not appear in binlog")
		case mysql_proto.FieldType_LONG_BLOB:
			return errors.New("Long blog type should not appear in binlog")
		case mysql_proto.FieldType_BLOB:
			fd, metadata, err = NewBlobFieldDescriptor(nullable, metadata)
		case mysql_proto.FieldType_VAR_STRING, mysql_proto.FieldType_STRING:
			fd = NewStringFieldDescriptor(realType, nullable, metaLength)
		case mysql_proto.FieldType_GEOMETRY:
			return errors.New("TODO")
		default:
			return errors.Newf("Unknown field type: %d", int(realType))
		}

		if err != nil {
			return err
		}

		t.columnDescriptors[idx] = NewColumnDescriptor(fd, idx)
	}

	if len(metadata) != 0 {
		// sanity check
		return errors.New("Not all column metadata is consumed")
	}

	return nil
}
