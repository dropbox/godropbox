package binlog

import (
	"bytes"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type TableMapEventSuite struct {
	EventParserSuite
}

var _ = Suite(&TableMapEventSuite{})

func (s *TableMapEventSuite) TestBasic(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id
			76, 0, 0, 0, 0, 0,
			// flags
			1, 0,
			// db name length
			8,
			// db name
			115, 104, 97, 114, 100, 55, 54, 55, 0,
			// table name length
			30,
			// table name
			99, 97, 109, 101, 114, 97, 95, 117, 112, 108,
			111, 97, 100, 95, 105, 110, 100, 101, 120, 95,
			115, 117, 109, 109, 97, 114, 121, 95, 118, 51, 0,
			// number of columns
			2,
			3, 2,
			// metadata size
			0,
			// null bits
			2})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	tm, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)
	c.Check(tm.TableId(), Equals, uint64(76))
	c.Check(tm.TableFlags(), Equals, uint16(1))
	c.Check(string(tm.DatabaseName()), Equals, "shard767")
	c.Check(string(tm.TableName()), Equals, "camera_upload_index_summary_v3")
	c.Check(tm.ColumnTypesBytes(), DeepEquals, []byte{3, 2})
	c.Check(tm.MetadataBytes(), DeepEquals, []byte{})
	c.Check(tm.NullColumnsBytes(), DeepEquals, []byte{2})

	descriptors := tm.ColumnDescriptors()
	c.Assert(len(descriptors), Equals, 2)
	c.Check(descriptors[0].Type(), Equals, mysql_proto.FieldType_LONG)
	c.Check(descriptors[0].IsNullable(), IsFalse)
	c.Check(descriptors[1].Type(), Equals, mysql_proto.FieldType_SHORT)
	c.Check(descriptors[1].IsNullable(), IsTrue)
}

func (s *TableMapEventSuite) TestTableWithMetadata(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id
			69, 0, 0, 0, 0, 0,
			// flags
			1, 0,
			// db name length
			26,
			// db name
			101, 100, 103, 101, 115, 116, 111, 114, 101, 95,
			115, 104, 97, 114, 100, 49, 49, 48, 49, 49,
			49, 49, 48, 49, 48, 49, 0,
			// table name length
			17,
			// table name
			101, 100, 103, 101, 100, 97, 116, 97, 95, 114,
			101, 118, 105, 115, 105, 111, 110, 0,
			// number of columns
			3,
			// column types
			254, 254, 8,
			// metadata size
			4,
			// metadata
			254, 16, 254, 16,
			// null bits
			0})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	tm, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)
	c.Check(tm.TableId(), Equals, uint64(69))
	c.Check(tm.TableFlags(), Equals, uint16(1))
	c.Check(string(tm.DatabaseName()), Equals, "edgestore_shard11011110101")
	c.Check(string(tm.TableName()), Equals, "edgedata_revision")

	descriptors := tm.ColumnDescriptors()
	c.Assert(len(descriptors), Equals, 3)
	c.Check(descriptors[0].Type(), Equals, mysql_proto.FieldType_STRING)
	c.Check(descriptors[1].Type(), Equals, mysql_proto.FieldType_STRING)
	c.Check(descriptors[2].Type(), Equals, mysql_proto.FieldType_LONGLONG)
}

func (s *TableMapEventSuite) TestAllColumnTypes(c *C) {
	type pair struct {
		fieldType mysql_proto.FieldType_Type
		realType  mysql_proto.FieldType_Type
		metadata  []byte
	}

	columnTypes := []pair{
		// TODO mysql_proto.FieldType_DECIMAL
		{mysql_proto.FieldType_TINY,
			mysql_proto.FieldType_TINY,
			nil},
		{mysql_proto.FieldType_SHORT,
			mysql_proto.FieldType_SHORT,
			nil},
		{mysql_proto.FieldType_LONG,
			mysql_proto.FieldType_LONG,
			nil},
		{mysql_proto.FieldType_FLOAT,
			mysql_proto.FieldType_FLOAT,
			[]byte{4}},
		{mysql_proto.FieldType_DOUBLE,
			mysql_proto.FieldType_DOUBLE,
			[]byte{8}},
		{mysql_proto.FieldType_NULL,
			mysql_proto.FieldType_NULL,
			nil},
		// TODO mysql_proto.FieldType_TIMESTAMP
		{mysql_proto.FieldType_LONGLONG,
			mysql_proto.FieldType_LONGLONG,
			nil},
		{mysql_proto.FieldType_INT24,
			mysql_proto.FieldType_INT24,
			nil},
		// TODO mysql_proto.FieldType_DATE
		// TODO mysql_proto.FieldType_TIME
		// TODO mysql_proto.FieldType_DATETIME
		// TODO mysql_proto.FieldType_YEAR
		// TODO mysql_proto.FieldType_NEWDATE
		{mysql_proto.FieldType_VARCHAR,
			mysql_proto.FieldType_VARCHAR,
			[]byte{255, 0}},
		// TODO mysql_proto.FieldType_BIT
		// TODO mysql_proto.FieldType_TIMESTAMP2
		// TODO mysql_proto.FieldType_DATETIME2
		// TODO mysql_proto.FieldType_TIME2
		// TODO mysql_proto.FieldType_NEWDECIMAL
		// NOTE: tiny / medium / long blobs don't exist in binlog
		{mysql_proto.FieldType_BLOB,
			mysql_proto.FieldType_BLOB,
			[]byte{4}},
		{mysql_proto.FieldType_VAR_STRING,
			mysql_proto.FieldType_VAR_STRING,
			[]byte{byte(mysql_proto.FieldType_VAR_STRING), 123}},
		{mysql_proto.FieldType_STRING,
			mysql_proto.FieldType_STRING,
			[]byte{byte(mysql_proto.FieldType_STRING), 123}},
		// TODO mysql_proto.FieldType_GEOMETRY
		// string -> varstring
		{mysql_proto.FieldType_STRING,
			mysql_proto.FieldType_VAR_STRING,
			[]byte{byte(mysql_proto.FieldType_VAR_STRING), 123}},
		// TODO mysql_proto.FieldType_ENUM
		// TODO mysql_proto.FieldType_SET
	}

	//
	// setup event
	//

	buf := &bytes.Buffer{}
	buf.Write([]byte{
		// table id
		222, 0, 0, 0, 0, 0,
		// flags
		1, 0,
		// db name length
		3,
		// db name
		'f', 'o', 'o', 0,
		// table name length,
		3,
		// table name length
		'b', 'a', 'r', 0})

	buf.WriteByte(byte(len(columnTypes)))

	for _, colType := range columnTypes {
		buf.WriteByte(byte(colType.fieldType))
	}
	metadataSize := 0
	for _, colType := range columnTypes {
		metadataSize += len(colType.metadata)
	}
	buf.WriteByte(byte(metadataSize))
	for _, colType := range columnTypes {
		if len(colType.metadata) > 0 {
			buf.Write(colType.metadata)
		}
	}
	for i := 0; i < (len(columnTypes)+7)/8; i++ {
		if i == 0 {
			buf.WriteByte(byte(0xff))
		} else {
			buf.WriteByte(byte(0))
		}
	}

	c.Log(buf.Bytes())
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		buf.Bytes())

	//
	// actual test
	//

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	tm, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)
	c.Check(tm.TableId(), Equals, uint64(222))
	c.Check(tm.TableFlags(), Equals, uint16(1))
	c.Check(string(tm.DatabaseName()), Equals, "foo")
	c.Check(string(tm.TableName()), Equals, "bar")

	descriptors := tm.ColumnDescriptors()
	c.Assert(len(descriptors), Equals, len(columnTypes))
	for i, colType := range columnTypes {
		cd := descriptors[i]
		c.Check(cd.Type(), Equals, colType.realType)
		if i < 8 {
			c.Check(cd.IsNullable(), IsTrue)
		} else {
			c.Check(cd.IsNullable(), IsFalse)
		}
	}
}
