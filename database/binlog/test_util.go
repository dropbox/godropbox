package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"

	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// This constructs a raw binlog event and returns its payload.
func CreateEventBytes(
	timestamp uint32,
	eventType uint8,
	serverId uint32,
	nextPosition uint32,
	flags uint16,
	data []byte) ([]byte, error) {

	totalLength := sizeOfBasicV4EventHeader + len(data)

	h := basicV4EventHeader{
		Timestamp:    timestamp,
		EventType:    eventType,
		ServerId:     serverId,
		EventLength:  uint32(totalLength),
		NextPosition: nextPosition,
		Flags:        flags,
	}

	buf := &bytes.Buffer{}

	err := binary.Write(buf, binary.LittleEndian, h)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func logName(prefix string, num int) string {
	return fmt.Sprintf("%s%06d", prefix, num)
}

// FakeLogFile provides a quick way to generate fake binlog data for testing.
type FakeLogFile struct {
	*bytes.Buffer

	nextPosition int
}

func NewFakeLogFile() *FakeLogFile {
	return &FakeLogFile{&bytes.Buffer{}, 0}
}

func (f *FakeLogFile) WriteLogFileMagic() {
	f.Write(logFileMagic)
}

func (f *FakeLogFile) WriteFDE() {
	data := []byte{
		// binlog version
		4, 0,
		// server version
		53, 46, 53, 46, 51, 52, 45, 51, 50, 46,
		48, 45, 108, 111, 103, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		// created timestamp
		0, 0, 0, 0,
		// total header size
		19,
		// fixed length data size per event type
		56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 84, 0, 4,
		26, 8, 0, 0, 0, 8, 8, 8, 2, 0}

	f.nextPosition += sizeOfBasicV4EventHeader + len(data)

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data)
	f.Write(e)
}

func (f *FakeLogFile) WriteXid(id uint64) {
	data := &bytes.Buffer{}
	binary.Write(data, binary.LittleEndian, id)

	f.nextPosition += sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_XID_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteRotate(prefix string, num int) {
	pos := uint64(4)

	data := &bytes.Buffer{}
	binary.Write(data, binary.LittleEndian, pos)
	data.WriteString(logName(prefix, num))

	f.nextPosition += sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_ROTATE_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteQueryWithParam(query string, dbName string) {

	data := &bytes.Buffer{}
	data.Write([]byte{
		0, 0, 0, 0, // thread id
		0, 0, 0, 0, // execute duration
		byte(len(dbName)), // db name length
		0, 0,              // error code
		0, 0, // status block size
	})
	data.WriteString(dbName)
	data.WriteByte(0)
	data.Write([]byte(query))

	f.nextPosition += sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_QUERY_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteQuery(query string) {
	f.WriteQueryWithParam(query, "db")
}

func (f *FakeLogFile) WriteBegin() {
	f.WriteQuery("BEGIN")
}

func (f *FakeLogFile) WriteRowsQuery(query string) {
	data := &bytes.Buffer{}
	data.WriteByte(byte(len(query)))
	data.WriteString(query) // Note: this mimics bug in mysql 5.6

	f.nextPosition += sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_ROWS_QUERY_LOG_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteTableMapWithParams(
	tableId int8,
	dbName string,
	tableName string) {

	buf := &bytes.Buffer{}
	buf.Write([]byte{
		// table id
		byte(tableId), 0, 0, 0, 0, 0,
		// flags
		1, 0,
	})

	buf.WriteByte(byte(len(dbName)))
	buf.Write([]byte(dbName))
	buf.WriteByte(0)

	buf.WriteByte(byte(len(tableName)))
	buf.Write([]byte(tableName))
	buf.WriteByte(0)

	buf.Write([]byte{
		// number of columns
		1,
		// a single long fields
		3,
		// metadata size
		0,
		// null bits
		2,
	})

	f.nextPosition += sizeOfBasicV4EventHeader + buf.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_TABLE_MAP_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		buf.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteTableMap() {
	f.WriteTableMapWithParams(
		0,
		"abc",
		"foo")
}

func (f *FakeLogFile) WriteInsertWithParam(value int, tableId int8) {
	data := &bytes.Buffer{}
	data.Write([]byte{
		// table id
		byte(tableId), 0, 0, 0, 0, 0,
		// flags
		0, 0,
		// empty variable size header
		2, 0,
		// number of columns
		1,
		// columns used bitmap
		1,
		// row data's "is null" bit map
		0,
	})
	binary.Write(data, binary.LittleEndian, uint32(value))

	f.nextPosition += sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_WRITE_ROWS_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteInsert(value int) {
	f.WriteInsertWithParam(value, 0)
}

func (f *FakeLogFile) WriteDeleteWithParam(value int, tableId int8) {
	data := &bytes.Buffer{}
	data.Write([]byte{
		// table id
		byte(tableId), 0, 0, 0, 0, 0,
		// flags
		0, 0,
		// empty variable size header
		2, 0,
		// number of columns
		1,
		// columns used bitmap
		1,
		// row data's "is null" bit map
		0,
	})
	binary.Write(data, binary.LittleEndian, uint32(value))

	f.nextPosition += sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_DELETE_ROWS_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteDelete(value int) {
	f.WriteDeleteWithParam(value, 0)
}

func (f *FakeLogFile) WriteUpdateWithParam(
	before int,
	after int,
	tableId int8) {

	data := &bytes.Buffer{}
	data.Write([]byte{
		// table id
		byte(tableId), 0, 0, 0, 0, 0,
		// flags
		0, 0,
		// empty variable size header
		2, 0,
		// number of columns
		1,
		// columns used bitmap (before image)
		1,
		// columns used bitmap (after image)
		1,
	})
	// row data's "is null" bit map
	data.WriteByte(0)
	binary.Write(data, binary.LittleEndian, uint32(before))
	// row data's "is null" bit map
	data.WriteByte(0)
	binary.Write(data, binary.LittleEndian, uint32(after))

	f.nextPosition += sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_UPDATE_ROWS_EVENT),
		uint32(1),
		uint32(f.nextPosition),
		uint16(1),
		data.Bytes())
	f.Write(e)
}

func (f *FakeLogFile) WriteUpdate(before int, after int) {
	f.WriteUpdateWithParam(before, after, 0)
}
