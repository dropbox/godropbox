package binlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type MockLogFile struct {
	logBuffer []byte

	mu sync.Mutex
}

type MockLogFileReader struct {
	file   *MockLogFile
	cursor int
}

// Statically verify that MockLogFile implements io.Reader.

var _ io.Reader = &MockLogFileReader{}

func NewMockLogFile() *MockLogFile {
	return &MockLogFile{
		logBuffer: make([]byte, 0),
	}
}

func (mlf *MockLogFile) GetReader() *MockLogFileReader {
	return newMockLogFileReader(mlf)
}

func (mlf *MockLogFile) Write(contents []byte) {
	mlf.logBuffer = append(mlf.logBuffer, contents...)
}

func (mlf *MockLogFile) WriteLogFileMagic() {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

	mlf.Write(logFileMagic)
}

func (mlf *MockLogFile) WriteFDE() {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

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

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + len(data)

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data)
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteXid(id uint64) {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

	data := &bytes.Buffer{}
	binary.Write(data, binary.LittleEndian, id)

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_XID_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteRotate(prefix string, num int) {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

	pos := uint64(4)

	data := &bytes.Buffer{}
	binary.Write(data, binary.LittleEndian, pos)
	data.WriteString(logName(prefix, num))

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_ROTATE_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteQueryWithParam(query string, dbName string) {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

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

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_QUERY_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteQuery(query string) {
	mlf.WriteQueryWithParam(query, "db")
}

func (mlf *MockLogFile) WriteBegin() {
	mlf.WriteQuery("BEGIN")
}

func (mlf *MockLogFile) WriteRowsQuery(query string) {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

	data := &bytes.Buffer{}
	data.WriteByte(byte(len(query)))
	data.WriteString(query) // Note: this mimics bug in mysql 5.6

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_ROWS_QUERY_LOG_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteTableMapWithParams(
	tableId int8,
	dbName string,
	tableName string) {

	mlf.mu.Lock()
	defer mlf.mu.Unlock()

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

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + buf.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_TABLE_MAP_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		buf.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteTableMap() {
	mlf.WriteTableMapWithParams(
		0,
		"abc",
		"foo")
}

func (mlf *MockLogFile) WriteInsertWithParam(value int, tableId int8) {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

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

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_WRITE_ROWS_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteInsert(value int) {
	mlf.WriteInsertWithParam(value, 0)
}

func (mlf *MockLogFile) WriteDeleteWithParam(value int, tableId int8) {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

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

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_DELETE_ROWS_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteDelete(value int) {
	mlf.WriteDeleteWithParam(value, 0)
}

func (mlf *MockLogFile) WriteUpdateWithParam(
	before int,
	after int,
	tableId int8) {

	mlf.mu.Lock()
	defer mlf.mu.Unlock()

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

	nextPosition := len(mlf.logBuffer) + sizeOfBasicV4EventHeader + data.Len()

	e, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_UPDATE_ROWS_EVENT),
		uint32(1),
		uint32(nextPosition),
		uint16(1),
		data.Bytes())
	mlf.Write(e)
}

func (mlf *MockLogFile) WriteUpdate(before int, after int) {
	mlf.WriteUpdateWithParam(before, after, 0)
}

func (mlf *MockLogFile) Copy() *MockLogFile {
	mlf.mu.Lock()
	defer mlf.mu.Unlock()

	bufferCopy := make([]byte, len(mlf.logBuffer))
	copied := copy(bufferCopy, mlf.logBuffer)
	if copied != len(bufferCopy) {
		panic("MockLogFile buffer copy failed")
	}

	return &MockLogFile{
		logBuffer: bufferCopy,
	}
}

func newMockLogFileReader(logFile *MockLogFile) *MockLogFileReader {
	return &MockLogFileReader{
		file:   logFile,
		cursor: 0,
	}
}

func (reader *MockLogFileReader) Read(p []byte) (n int, err error) {
	reader.file.mu.Lock()
	defer reader.file.mu.Unlock()

	if reader.cursor+len(p) > len(reader.file.logBuffer) {
		// We can't read anything.
		return 0, io.EOF
	}

	// We can read something.
	copied := copy(p, reader.file.logBuffer[reader.cursor:])
	if copied != len(p) {
		panic("MockLogFileReader read failed")
	}
	reader.cursor += len(p)
	return len(p), nil
}
