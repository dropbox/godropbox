package binlog

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

const testRowsTableId = 42

type testTableContext struct {
	columns []ColumnDescriptor
}

func newTestTableContext() TableContext {
	c := &testTableContext{
		columns: make([]ColumnDescriptor, 0, 0),
	}
	c.columns = append(
		c.columns,
		NewColumnDescriptor(NewTinyFieldDescriptor(true), 0))
	c.columns = append(
		c.columns,
		NewColumnDescriptor(NewShortFieldDescriptor(false), 1))
	c.columns = append(
		c.columns,
		NewColumnDescriptor(NewInt24FieldDescriptor(true), 2))
	c.columns = append(
		c.columns,
		NewColumnDescriptor(NewLongFieldDescriptor(true), 3))
	c.columns = append(
		c.columns,
		NewColumnDescriptor(NewLongLongFieldDescriptor(true), 4))

	return c
}

func (c *testTableContext) TableId() uint64 {
	return testRowsTableId
}

func (c *testTableContext) TableFlags() uint16 {
	return 1
}

func (c *testTableContext) DatabaseName() []byte {
	return []byte("database")
}

func (c *testTableContext) TableName() []byte {
	return []byte("table")
}

func (c *testTableContext) NumColumns() int {
	return len(c.columns)
}

func (c *testTableContext) ColumnDescriptors() []ColumnDescriptor {
	return c.columns
}

type RowsEventSuite struct {
	EventParserSuite

	context TableContext
}

var _ = Suite(&RowsEventSuite{})

func (s *RowsEventSuite) SetUpTest(c *C) {
	s.EventParserSuite.SetUpTest(c)

	s.context = newTestTableContext()
	s.parsers.SetTableContext(s.context)
}

func (s *RowsEventSuite) TestWriteRowsV1(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_WRITE_ROWS_EVENT_V1,
		uint16(0),
		[]byte{
			// table id
			testRowsTableId, 0, 0, 0, 0, 0,
			// table flags,
			14, 0,
			// # known columns
			5,
			// used column bits
			(1 + 2 + 8 + 16), // skipping the int24 column

			// ROW DATA:

			// Row 1: tiny = 1; short = 2; long = 4; longlong = 8
			0,    // null column bits
			1,    // tiny
			2, 0, // short
			4, 0, 0, 0, // long
			8, 0, 0, 0, 0, 0, 0, 0, // longlong

			// Row 2: tiny = nil; short = 20; long = 40; longlong = nil
			(1 + 8), // null column bits
			20, 0,   // short
			40, 0, 0, 0, // long

			// Row 3: tiny = 11; short = 22; long = nil; longlong = nil
			(4 + 8), // null column bits
			11,      // tiny
			22, 0,   // short
		})

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*WriteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V1)
	c.Check(w.TableId(), Equals, uint64(testRowsTableId))
	c.Check(w.RowsFlags(), Equals, uint16(14))
	c.Check(w.NumColumns(), Equals, 5)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	descriptors := s.context.ColumnDescriptors()
	expectedUsed := []ColumnDescriptor{
		descriptors[0],
		descriptors[1],
		descriptors[3],
		descriptors[4],
	}

	c.Check(w.UsedColumns(), DeepEquals, expectedUsed)

	rows := w.InsertedRows()
	c.Assert(len(rows), Equals, 3)

	expectedRow1 := RowValues{uint64(1), uint64(2), uint64(4), uint64(8)}
	c.Check(rows[0], DeepEquals, expectedRow1)

	expectedRow2 := RowValues{nil, uint64(20), uint64(40), nil}
	c.Check(rows[1], DeepEquals, expectedRow2)

	expectedRow3 := RowValues{uint64(11), uint64(22), nil, nil}
	c.Check(rows[2], DeepEquals, expectedRow3)
}

func (s *RowsEventSuite) TestWriteRowsV2(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_WRITE_ROWS_EVENT,
		uint16(0),
		[]byte{
			// table id
			testRowsTableId, 0, 0, 0, 0, 0,
			// table flags,
			14, 0,
			// extra metadata (total) length + 2
			7, 0,
			0, // RW_V_EXTRAINFO_TAG
			3, // info blob length
			'f', 'o', 'o',
			// # known columns
			5,
			// used column bits
			(1 + 2 + 8 + 16), // skipping the int24 column

			// ROW DATA:

			// Row 1: tiny = 1; short = 2; long = 4; longlong = 8
			0,    // null column bits
			1,    // tiny
			2, 0, // short
			4, 0, 0, 0, // long
			8, 0, 0, 0, 0, 0, 0, 0, // longlong
		})

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*WriteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V2)
	c.Check(w.TableId(), Equals, uint64(testRowsTableId))
	c.Check(w.RowsFlags(), Equals, uint16(14))
	c.Check(w.NumColumns(), Equals, 5)
	c.Check(w.ExtraRowInfoBytes(), DeepEquals, []byte("foo"))

	descriptors := s.context.ColumnDescriptors()
	expectedUsed := []ColumnDescriptor{
		descriptors[0],
		descriptors[1],
		descriptors[3],
		descriptors[4],
	}

	c.Check(w.UsedColumns(), DeepEquals, expectedUsed)

	rows := w.InsertedRows()
	c.Assert(len(rows), Equals, 1)

	expectedRow1 := RowValues{uint64(1), uint64(2), uint64(4), uint64(8)}
	c.Check(rows[0], DeepEquals, expectedRow1)
}

func (s *RowsEventSuite) TestUpdateRowsV1(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_UPDATE_ROWS_EVENT_V1,
		uint16(0),
		[]byte{
			// table id
			testRowsTableId, 0, 0, 0, 0, 0,
			// table flags,
			14, 0,
			// # known columns
			5,
			// before image used columns bits
			(1 + 4 + 16), // tiny, int24, and longlong columns
			// after image used columns bits
			(2 + 8), // short & long columns

			// ROW DATA:

			// Row 1
			// before image: tiny = 1; int24 = 3; longlong = 8
			0,       // null bits
			1,       // tiny
			3, 0, 0, // int24
			8, 0, 0, 0, 0, 0, 0, 0, // longlong
			// after image: short = 2; long = 4
			0,    // null bits
			2, 0, // short
			4, 0, 0, 0, // long

			// Row 2:
			// before image: tiny = nil; int24 = 30; longlong = nil;
			(1 + 4),  // null column bits
			30, 0, 0, // int24
			// after image: short = 20; long = nil
			2,     // null column bits
			20, 0, // short

			// Row 3
			// before image: tiny = 11; int24 = nil; longlong = 88
			2,                       // null column bits
			11,                      // tiny
			88, 0, 0, 0, 0, 0, 0, 0, // short
			// after image: short = 22; long = 44
			0, // null column bits
			22, 0,
			44, 0, 0, 0,
		})

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*UpdateRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V1)
	c.Check(w.TableId(), Equals, uint64(testRowsTableId))
	c.Check(w.RowsFlags(), Equals, uint16(14))
	c.Check(w.NumColumns(), Equals, 5)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	descriptors := s.context.ColumnDescriptors()
	expectedBefore := []ColumnDescriptor{
		descriptors[0],
		descriptors[2],
		descriptors[4],
	}
	c.Check(w.BeforeImageUsedColumns(), DeepEquals, expectedBefore)

	expectedAfter := []ColumnDescriptor{
		descriptors[1],
		descriptors[3],
	}
	c.Check(w.AfterImageUsedColumns(), DeepEquals, expectedAfter)

	rows := w.UpdatedRows()
	c.Assert(len(rows), Equals, 3)

	expectedBefore1 := RowValues{uint64(1), uint64(3), uint64(8)}
	expectedAfter1 := RowValues{uint64(2), uint64(4)}
	c.Check(rows[0].BeforeImage, DeepEquals, expectedBefore1)
	c.Check(rows[0].AfterImage, DeepEquals, expectedAfter1)

	expectedBefore2 := RowValues{nil, uint64(30), nil}
	expectedAfter2 := RowValues{uint64(20), nil}
	c.Check(rows[1].BeforeImage, DeepEquals, expectedBefore2)
	c.Check(rows[1].AfterImage, DeepEquals, expectedAfter2)

	expectedBefore3 := RowValues{uint64(11), nil, uint64(88)}
	expectedAfter3 := RowValues{uint64(22), uint64(44)}
	c.Check(rows[2].BeforeImage, DeepEquals, expectedBefore3)
	c.Check(rows[2].AfterImage, DeepEquals, expectedAfter3)
}

func (s *RowsEventSuite) TestUpdateRowsV2(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_UPDATE_ROWS_EVENT,
		uint16(0),
		[]byte{
			// table id
			testRowsTableId, 0, 0, 0, 0, 0,
			// table flags,
			14, 0,
			// extra metadata (total) length + 2
			7, 0,
			0, // RW_V_EXTRAINFO_TAG
			3, // info blob length
			'f', 'o', 'o',
			// # known columns
			5,
			// before image used columns bits
			(1 + 4 + 16), // tiny, int24, and longlong columns
			// after image used columns bits
			(2 + 8), // short & long columns

			// ROW DATA:

			// Row 1
			// before image: tiny = 1; int24 = 3; longlong = 8
			0,       // null bits
			1,       // tiny
			3, 0, 0, // int24
			8, 0, 0, 0, 0, 0, 0, 0, // longlong
			// after image: short = 2; long = 4
			0,    // null bits
			2, 0, // short
			4, 0, 0, 0, // long
		})

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*UpdateRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V2)
	c.Check(w.TableId(), Equals, uint64(testRowsTableId))
	c.Check(w.RowsFlags(), Equals, uint16(14))
	c.Check(w.NumColumns(), Equals, 5)
	c.Check(w.ExtraRowInfoBytes(), DeepEquals, []byte("foo"))

	descriptors := s.context.ColumnDescriptors()
	expectedBefore := []ColumnDescriptor{
		descriptors[0],
		descriptors[2],
		descriptors[4],
	}
	c.Check(w.BeforeImageUsedColumns(), DeepEquals, expectedBefore)

	expectedAfter := []ColumnDescriptor{
		descriptors[1],
		descriptors[3],
	}
	c.Check(w.AfterImageUsedColumns(), DeepEquals, expectedAfter)

	rows := w.UpdatedRows()
	c.Assert(len(rows), Equals, 1)

	expectedBefore1 := RowValues{uint64(1), uint64(3), uint64(8)}
	expectedAfter1 := RowValues{uint64(2), uint64(4)}
	c.Check(rows[0].BeforeImage, DeepEquals, expectedBefore1)
	c.Check(rows[0].AfterImage, DeepEquals, expectedAfter1)
}

func (s *RowsEventSuite) TestDeleteRowsV1(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_DELETE_ROWS_EVENT_V1,
		uint16(0),
		[]byte{
			// table id
			testRowsTableId, 0, 0, 0, 0, 0,
			// table flags,
			14, 0,
			// # known columns
			5,
			// used column bits
			(1 + 2 + 8 + 16), // skipping the int24 column
			// ROW DATA:
			// Row 1: tiny = 1; short = 2; long = 4; longlong = 8
			0,    // null column bits
			1,    // tiny
			2, 0, // short
			4, 0, 0, 0, // long
			8, 0, 0, 0, 0, 0, 0, 0, // longlong
			// Row 2: tiny = nil; short = 20; long = 40; longlong = nil
			(1 + 8), // null column bits
			20, 0,   // short
			40, 0, 0, 0, // long
			// Row 3: tiny = 11; short = 22; long = nil; longlong = nil
			(4 + 8), // null column bits
			11,      // tiny
			22, 0,   // short
		})

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*DeleteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V1)
	c.Check(w.TableId(), Equals, uint64(testRowsTableId))
	c.Check(w.RowsFlags(), Equals, uint16(14))
	c.Check(w.NumColumns(), Equals, 5)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	descriptors := s.context.ColumnDescriptors()
	expectedUsed := []ColumnDescriptor{
		descriptors[0],
		descriptors[1],
		descriptors[3],
		descriptors[4],
	}

	c.Check(w.UsedColumns(), DeepEquals, expectedUsed)

	rows := w.DeletedRows()
	c.Assert(len(rows), Equals, 3)

	expectedRow1 := RowValues{uint64(1), uint64(2), uint64(4), uint64(8)}
	c.Check(rows[0], DeepEquals, expectedRow1)

	expectedRow2 := RowValues{nil, uint64(20), uint64(40), nil}
	c.Check(rows[1], DeepEquals, expectedRow2)

	expectedRow3 := RowValues{uint64(11), uint64(22), nil, nil}
	c.Check(rows[2], DeepEquals, expectedRow3)
}

func (s *RowsEventSuite) TestDeleteRowsV2(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_DELETE_ROWS_EVENT,
		uint16(0),
		[]byte{
			// table id
			testRowsTableId, 0, 0, 0, 0, 0,
			// table flags,
			14, 0,
			// extra metadata (total) length + 2
			7, 0,
			0, // RW_V_EXTRAINFO_TAG
			3, // info blob length
			'f', 'o', 'o',
			// # known columns
			5,
			// used column bits
			(1 + 2 + 8 + 16), // skipping the int24 column

			// ROW DATA:

			// Row 1: tiny = 1; short = 2; long = 4; longlong = 8
			0,    // null column bits
			1,    // tiny
			2, 0, // short
			4, 0, 0, 0, // long
			8, 0, 0, 0, 0, 0, 0, 0, // longlong
		})

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*DeleteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V2)
	c.Check(w.TableId(), Equals, uint64(testRowsTableId))
	c.Check(w.RowsFlags(), Equals, uint16(14))
	c.Check(w.NumColumns(), Equals, 5)
	c.Check(w.ExtraRowInfoBytes(), DeepEquals, []byte("foo"))

	descriptors := s.context.ColumnDescriptors()
	expectedUsed := []ColumnDescriptor{
		descriptors[0],
		descriptors[1],
		descriptors[3],
		descriptors[4],
	}

	c.Check(w.UsedColumns(), DeepEquals, expectedUsed)

	rows := w.DeletedRows()
	c.Assert(len(rows), Equals, 1)

	expectedRow1 := RowValues{uint64(1), uint64(2), uint64(4), uint64(8)}
	c.Check(rows[0], DeepEquals, expectedRow1)
}

func (s *RowsEventSuite) TestRealWriteRowsV1(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			35, 0, 0, 0, 0, 0,
			1, 0,
			// db length + name
			8,
			115, 104, 97, 114, 100, 55, 54, 54, 0,
			// table length + name
			22,
			100, 101, 114, 105, 118, 101, 100, 95, 109, 101, 116,
			97, 100, 97, 116, 97, 95, 115, 116, 111, 114, 101, 0,
			// # columns + column types
			7,
			3, 254, 1, 252, 1, 1, 252,
			// metadata
			4,
			254, 45, 2, 3,
			// null bits
			8})

	s.WriteEvent(
		mysql_proto.LogEventType_WRITE_ROWS_EVENT_V1,
		uint16(0),
		[]byte{
			// table id + row flags
			35, 0, 0, 0, 0, 0,
			1, 0,
			// # columns + used columns bit map
			7, 255,
			// rows data
			136,
			// col 0
			41, 215, 45, 12,
			// col 1
			43,
			56, 97, 115, 99, 87, 73, 95, 66, 106, 57,
			99, 73, 118, 107, 116, 69, 89, 79, 85, 102,
			48, 48, 49, 110, 98, 90, 77, 79, 103, 89,
			54, 84, 107, 87, 101, 79, 88, 109, 80, 75,
			48, 122, 115,
			29, 1, 1,
			// col 6
			0, 0, 0})

	c.Log(s.src.Bytes())

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	context, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)

	// sanity check
	c.Check(string(context.DatabaseName()), Equals, "shard766")
	c.Check(string(context.TableName()), Equals, "derived_metadata_store")

	s.parsers.SetTableContext(context)

	event, err = s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*WriteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V1)
	c.Check(w.TableId(), Equals, uint64(35))
	c.Check(w.RowsFlags(), Equals, uint16(1))
	c.Check(w.NumColumns(), Equals, 7)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	rows := w.InsertedRows()
	c.Check(len(rows), Equals, 1)

	// dump from mysqlbinlog -vv
	expected := RowValues{
		uint64(204330793),
		[]byte("8ascWI_Bj9cIvktEYOUf001nbZMOgY6TkWeOXmPK0zs\x00\x00"),
		uint64(29),
		nil,
		uint64(1),
		uint64(1),
		[]byte(""),
	}
	c.Check(rows[0], DeepEquals, expected)
}

func (s *RowsEventSuite) TestRealWriteRowsV2(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			210, 1, 0, 0, 0, 0,
			1, 0,
			// db name
			20,
			104, 100, 98, 95, 98, 108, 111, 99, 107, 95,
			115, 106, 100, 95, 115, 104, 97, 114, 100, 55, 0,
			// table name
			6,
			104, 97, 115, 104, 101, 115, 0,
			// # cols + col types
			9,
			15, 3, 3, 3, 5, 254, 1, 254, 3,
			// metadata
			7,
			255, 0, 8, 254, 9, 254, 16,
			// null bits
			232, 1})

	s.WriteEvent(
		mysql_proto.LogEventType_WRITE_ROWS_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			210, 1, 0, 0, 0, 0,
			1, 0,
			// extra info len (empty)
			2, 0,
			// # cols + cols used bit map
			9,
			255, 255,
			// rows data
			96, 254, 32, 7, 80, 26, 93, 139, 20, 19, 141, 154, 102, 140,
			210, 70, 53, 127, 30, 201, 239, 32, 86, 77, 216, 158, 64, 234,
			60, 192, 143, 192, 139, 107, 55, 244, 3, 0, 0, 234, 215, 20, 0,
			66, 149, 55, 9, 164, 112, 13, 162, 110, 209, 212, 65, 16, 217,
			27, 111, 8, 193, 244, 54, 24, 203, 112, 44, 11, 123, 78, 168,
			80, 3, 0, 0, 0})

	c.Log(s.src.Bytes())

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	context, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)

	// sanity check
	c.Check(string(context.DatabaseName()), Equals, "hdb_block_sjd_shard7")
	c.Check(string(context.TableName()), Equals, "hashes")

	s.parsers.SetTableContext(context)

	event, err = s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*WriteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V2)
	c.Check(w.TableId(), Equals, uint64(466))
	c.Check(w.RowsFlags(), Equals, uint16(1))
	c.Check(w.NumColumns(), Equals, 9)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	rows := w.InsertedRows()
	c.Check(len(rows), Equals, 1)

	// dump from mysqlbinlog -vv
	expected := RowValues{
		// '\x07P\x1a]?\x14\x13??f??F5\x1e?? VM؞@?<????k7'
		[]byte{
			7, 80, 26, 93, 139, 20, 19, 141, 154, 102, 140, 210, 70,
			53, 127, 30, 201, 239, 32, 86, 77, 216, 158, 64, 234,
			60, 192, 143, 192, 139, 107, 55},
		uint64(1012),
		uint64(1365994),
		uint64(154637634),
		float64(1397078664.2100000381),
		nil,
		nil,
		// '?\x1bo\x08??6\x18?p,\x0b{N?P'
		[]byte{
			217, 27, 111, 8, 193, 244, 54, 24, 203, 112, 44, 11, 123,
			78, 168, 80},
		uint64(3),
	}
	c.Check(rows[0], DeepEquals, expected)
}

func (s *RowsEventSuite) TestRealUpdateRowsV1(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			46, 0, 0, 0, 0, 0,
			1, 0,
			// db name
			8,
			115, 104, 97, 114, 100, 55, 54, 54, 0,
			// table name
			30,
			99, 97, 109, 101, 114, 97, 95, 117, 112, 108,
			111, 97, 100, 95, 105, 110, 100, 101, 120, 95,
			115, 117, 109, 109, 97, 114, 121, 95, 118, 51, 0,
			// # cols + cols type
			3,
			3, 3, 3,
			// metadata len + null bits
			0,
			0})

	s.WriteEvent(
		mysql_proto.LogEventType_UPDATE_ROWS_EVENT_V1,
		uint16(0),
		[]byte{
			// table id + flags
			46, 0, 0, 0, 0, 0,
			1, 0,
			// # cols + used cols bit maps
			3,
			255, // before image
			255, // after image
			// row data
			248, 236, 121, 59, 3, 176, 71, 0, 0, 177, 71, 0, 0,
			248, 236, 121, 59, 3, 177, 71, 0, 0, 177, 71, 0, 0})

	c.Log(s.src.Bytes())

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	context, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)

	// sanity check
	c.Check(string(context.DatabaseName()), Equals, "shard766")
	c.Check(
		string(context.TableName()),
		Equals,
		"camera_upload_index_summary_v3")

	s.parsers.SetTableContext(context)

	event, err = s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*UpdateRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V1)
	c.Check(w.TableId(), Equals, uint64(46))
	c.Check(w.RowsFlags(), Equals, uint16(1))
	c.Check(w.NumColumns(), Equals, 3)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	rows := w.UpdatedRows()
	c.Check(len(rows), Equals, 1)

	// dump from mysqlbinlog -vv
	expectedBefore := RowValues{
		uint64(54229484),
		uint64(18352),
		uint64(18353),
	}
	expectedAfter := RowValues{
		uint64(54229484),
		uint64(18353),
		uint64(18353),
	}
	c.Check(rows[0].BeforeImage, DeepEquals, expectedBefore)
	c.Check(rows[0].AfterImage, DeepEquals, expectedAfter)
}

func (s *RowsEventSuite) TestRealUpdateRowsV2(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			210, 1, 0, 0, 0, 0,
			1, 0,
			// db name
			20,
			104, 100, 98, 95, 98, 108, 111, 99, 107, 95, 115,
			106, 100, 95, 115, 104, 97, 114, 100, 55, 0,
			// table name
			6,
			104, 97, 115, 104, 101, 115, 0,
			// # cols + col types
			9,
			15, 3, 3, 3, 5, 254, 1, 254, 3,
			// metadata
			7,
			255, 0, 8, 254, 9, 254, 16,
			// null bits
			232, 1})

	s.WriteEvent(
		mysql_proto.LogEventType_UPDATE_ROWS_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			210, 1, 0, 0, 0, 0,
			1, 0,
			// extra info len (empty)
			2, 0,
			// # cols + used column bits
			9,
			255, 255, // before image
			255, 255, // after image
			// row data
			96, 254, 32, 7, 81, 209, 206, 212, 199, 88, 205, 182, 140,
			96, 98, 146, 43, 103, 157, 156, 162, 96, 237, 119, 138, 253,
			71, 55, 111, 174, 255, 47, 182, 44, 74, 243, 3, 0, 0, 131, 51,
			20, 0, 126, 28, 144, 212, 135, 254, 10, 40, 219, 209, 212, 65,
			15, 215, 73, 111, 70, 11, 242, 132, 150, 26, 98, 20, 49, 20,
			209, 61, 1, 0, 0, 0, 96, 254, 32, 7, 81, 209, 206, 212, 199, 88,
			205, 182, 140, 96, 98, 146, 43, 103, 157, 156, 162, 96, 237, 119,
			138, 253, 71, 55, 111, 174, 255, 47, 182, 44, 74, 243, 3, 0, 0,
			131, 51, 20, 0, 126, 28, 144, 212, 135, 254, 10, 40, 219, 209,
			212, 65, 15, 215, 73, 111, 70, 11, 242, 132, 150, 26, 98, 20,
			49, 20, 209, 61, 3, 0, 0, 0})

	c.Log(s.src.Bytes())

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	context, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)

	// sanity check
	c.Check(string(context.DatabaseName()), Equals, "hdb_block_sjd_shard7")
	c.Check(string(context.TableName()), Equals, "hashes")

	s.parsers.SetTableContext(context)

	event, err = s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*UpdateRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V2)
	c.Check(w.TableId(), Equals, uint64(466))
	c.Check(w.RowsFlags(), Equals, uint16(1))
	c.Check(w.NumColumns(), Equals, 9)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	rows := w.UpdatedRows()
	c.Check(len(rows), Equals, 1)

	// dump from mysqlbinlog -vv
	expectedBefore := RowValues{
		// '\x07Q????XͶ?`b?+g???`?w??G7o??/?,J'
		[]byte{
			7, 81, 209, 206, 212, 199, 88, 205, 182, 140, 96, 98, 146, 43,
			103, 157, 156, 162, 96, 237, 119, 138, 253, 71, 55, 111, 174,
			255, 47, 182, 44, 74},
		uint64(1011),
		uint64(1323907),
		// -728753026 (3566214270)
		uint64(3566214270),
		float64(1397189792.1717851162),
		nil,
		nil,
		// '?IoF\x0b?\x1ab\x141\x14?='
		[]byte{
			215, 73, 111, 70, 11, 242, 132, 150, 26, 98, 20, 49, 20, 209, 61, 0},
		uint64(1),
	}
	expectedAfter := RowValues{
		// '\x07Q????XͶ?`b?+g???`?w??G7o??/?,J'
		[]byte{
			7, 81, 209, 206, 212, 199, 88, 205, 182, 140, 96, 98, 146, 43,
			103, 157, 156, 162, 96, 237, 119, 138, 253, 71, 55, 111, 174,
			255, 47, 182, 44, 74},
		uint64(1011),
		uint64(1323907),
		// -728753026 (3566214270)
		uint64(3566214270),
		float64(1397189792.1717851162),
		nil,
		nil,
		// '?IoF\x0b?\x1ab\x141\x14?='
		[]byte{
			215, 73, 111, 70, 11, 242, 132, 150, 26, 98, 20, 49, 20, 209, 61, 0},
		uint64(3),
	}
	c.Check(rows[0].BeforeImage, DeepEquals, expectedBefore)
	c.Check(rows[0].AfterImage, DeepEquals, expectedAfter)
}

func (s *RowsEventSuite) TestRealDeleteRowsV1(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			87, 0, 0, 0, 0, 0,
			1, 0,
			// db name
			8,
			115, 104, 97, 114, 100, 55, 54, 56, 0,
			// table name
			15,
			116, 114, 105, 103, 103, 101, 114, 115, 95,
			102, 111, 114, 95, 110, 115, 0,
			// # cols + col type
			5,
			3, 3, 3, 3, 3,
			// metadata len
			0,
			// null bits
			24})

	s.WriteEvent(
		mysql_proto.LogEventType_DELETE_ROWS_EVENT_V1,
		uint16(0),
		[]byte{
			// table id + flags
			87, 0, 0, 0, 0, 0,
			1, 0,
			// # cols
			5,
			// used cols bits
			255,
			// row data
			224, 215, 170, 9, 0, 164, 73, 229, 13, 61, 113,
			98, 11, 116, 42, 168, 8, 59, 7, 0, 0})

	c.Log(s.src.Bytes())

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	context, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)

	// sanity check
	c.Check(string(context.DatabaseName()), Equals, "shard768")
	c.Check(string(context.TableName()), Equals, "triggers_for_ns")

	s.parsers.SetTableContext(context)

	event, err = s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*DeleteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V1)
	c.Check(w.TableId(), Equals, uint64(87))
	c.Check(w.RowsFlags(), Equals, uint16(1))
	c.Check(w.NumColumns(), Equals, 5)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	rows := w.DeletedRows()
	c.Check(len(rows), Equals, 1)

	// dump from mysqlbinlog -vv
	expected := RowValues{
		uint64(633559),
		uint64(233130404),
		uint64(191000893),
		uint64(145238644),
		uint64(1851),
	}
	c.Check(rows[0], DeepEquals, expected)
}

func (s *RowsEventSuite) TestRealDeleteRowsV2(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_TABLE_MAP_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			98, 0, 0, 0, 0, 0,
			1, 0,
			// db name
			26,
			101, 100, 103, 101, 115, 116, 111, 114, 101, 95, 115, 104, 97,
			114, 100, 49, 49, 49, 49, 49, 49, 48, 49, 48, 48, 49, 0,
			// table name
			14,
			101, 100, 103, 101, 100, 97, 116, 97, 95,
			105, 110, 100, 101, 120, 0,
			// # cols + col types
			3,
			254, 15, 8,
			// metadata
			4,
			254, 16, 0, 2,
			// null bits
			0})

	s.WriteEvent(
		mysql_proto.LogEventType_DELETE_ROWS_EVENT,
		uint16(0),
		[]byte{
			// table id + flags
			98, 0, 0, 0, 0, 0,
			1, 0,
			// extra info len (empty)
			2, 0,
			// # cols + used cols bit map
			3,
			255,
			// rows data
			248, 16, 76, 10, 25, 139, 200, 35, 36, 129, 0, 0, 0, 0, 0, 0, 0,
			9, 110, 0, 253, 57, 243, 139, 140, 46, 42, 192, 0, 255, 0, 255, 0,
			255, 0, 255, 0, 255, 0, 255, 0, 255, 3, 0, 0, 1, 0, 0, 1, 0, 0,
			128, 0, 255, 0, 255, 0, 255, 0, 0, 86, 45, 45, 48, 71, 78, 45, 68,
			89, 109, 88, 45, 45, 45, 52, 52, 86, 45, 117, 69, 57, 45, 118, 56,
			70, 78, 48, 52, 55, 85, 101, 107, 56, 100, 56, 83, 51, 89, 100,
			82, 118, 67, 90, 0, 0, 253, 57, 243, 139, 140, 46, 42, 192, 0,
			255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 202, 0, 0,
			128, 138, 219, 236, 15, 232, 105, 19, 248, 16, 76, 10, 25, 139,
			200, 35, 36, 129, 0, 0, 0, 0, 0, 0, 0, 9, 110, 0, 253, 57, 243,
			139, 140, 46, 42, 192, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0,
			255, 0, 255, 3, 0, 0, 1, 0, 0, 1, 0, 0, 128, 0, 255, 0, 255, 0,
			255, 0, 0, 86, 45, 45, 48, 71, 80, 50, 85, 109, 86, 49, 45, 45,
			45, 52, 52, 99, 67, 122, 57, 45, 45, 118, 56, 70, 78, 48, 52, 55,
			85, 101, 107, 56, 100, 56, 83, 51, 86, 52, 87, 52, 104, 82, 0, 0,
			253, 57, 243, 139, 140, 46, 42, 192, 0, 255, 0, 255, 0, 255, 0,
			255, 0, 255, 0, 255, 0, 255, 201, 0, 0, 127, 138, 219, 236, 15,
			232, 105, 19, 248, 16, 76, 10, 25, 139, 200, 35, 36, 129, 0, 0, 0,
			0, 0, 0, 0, 9, 110, 0, 253, 57, 243, 139, 140, 46, 42, 192, 0, 255,
			0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 3, 0, 0, 1, 0, 0,
			1, 0, 0, 128, 0, 255, 0, 255, 0, 255, 0, 0, 86, 45, 45, 48, 71,
			80, 70, 109, 107, 80, 88, 45, 45, 45, 52, 52, 100, 50, 65, 48,
			101, 45, 118, 56, 70, 78, 48, 52, 55, 85, 101, 107, 56, 100, 56,
			83, 51, 97, 56, 50, 98, 120, 107, 0, 0, 253, 57, 243, 139, 140,
			46, 42, 192, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0,
			255, 200, 0, 0, 126, 138, 219, 236, 15, 232, 105, 19})

	c.Log(s.src.Bytes())

	event, err := s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	context, ok := event.(*TableMapEvent)
	c.Assert(ok, IsTrue)

	// sanity check
	c.Check(
		string(context.DatabaseName()),
		Equals,
		"edgestore_shard11111101001")
	c.Check(string(context.TableName()), Equals, "edgedata_index")

	s.parsers.SetTableContext(context)

	event, err = s.NextEvent()
	c.Log(err)
	c.Assert(err, IsNil)

	w, ok := event.(*DeleteRowsEvent)
	c.Assert(ok, IsTrue)

	c.Assert(w.Version(), Equals, mysql_proto.RowsEventVersion_V2)
	c.Check(w.TableId(), Equals, uint64(98))
	c.Check(w.RowsFlags(), Equals, uint16(1))
	c.Check(w.NumColumns(), Equals, 3)
	c.Check(w.ExtraRowInfoBytes(), IsNil)

	rows := w.DeletedRows()
	c.Check(len(rows), Equals, 3)

	// dump from mysqlbinlog -vv
	expected1 := RowValues{
		// 'L\x0a\x19??#$?\x00\x00\x00\x00\x00\x00\x00\x09'
		[]byte{76, 10, 25, 139, 200, 35, 36, 129, 0, 0, 0, 0, 0, 0, 0, 9},
		// '?9?.*?\x00?\x00?\x00?\x00?\x00?\x00?\x00?\x03\x00\x00\x01'
		// '\x00\x00\x01\x00\x00?\x00?\x00?\x00?\x00\x00V--0GN-DYmX---44V'
		// '-uE9-v8FN047Uek8d8S3YdRvCZ\x00\x00?9?.*?\x00?\x00?\x00?\x00?'
		//'\x00?\x00?\x00??\x00\x00
		[]byte{
			253, 57, 243, 139, 140, 46, 42, 192, 0, 255, 0, 255, 0, 255, 0,
			255, 0, 255, 0, 255, 0, 255, 3, 0, 0, 1, 0, 0, 1, 0, 0, 128, 0,
			255, 0, 255, 0, 255, 0, 0, 86, 45, 45, 48, 71, 78, 45, 68, 89,
			109, 88, 45, 45, 45, 52, 52, 86, 45, 117, 69, 57, 45, 118, 56,
			70, 78, 48, 52, 55, 85, 101, 107, 56, 100, 56, 83, 51, 89, 100,
			82, 118, 67, 90, 0, 0, 253, 57, 243, 139, 140, 46, 42, 192, 0,
			255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 202, 0, 0},
		uint64(1398904314371213952),
	}
	c.Check(rows[0], DeepEquals, expected1)

	expected2 := RowValues{
		// 'L\x0a\x19??#$?\x00\x00\x00\x00\x00\x00\x00\x09'
		[]byte{76, 10, 25, 139, 200, 35, 36, 129, 0, 0, 0, 0, 0, 0, 0, 9},
		// '?9?.*?\x00?\x00?\x00?\x00?\x00?\x00?\x00?\x03\x00\x00\x01\x00'
		// '\x00\x01\x00\x00?\x00?\x00?\x00?\x00\x00V--0GP2UmV1---44cCz9--'
		// 'v8FN047Uek8d8S3V4W4hR\x00\x00?9?.*?\x00?\x00?\x00?\x00?\x00?\x00?'
		// '\x00??\x00\x00'
		[]byte{
			253, 57, 243, 139, 140, 46, 42, 192, 0, 255, 0, 255, 0, 255, 0,
			255, 0, 255, 0, 255, 0, 255, 3, 0, 0, 1, 0, 0, 1, 0, 0, 128, 0,
			255, 0, 255, 0, 255, 0, 0, 86, 45, 45, 48, 71, 80, 50, 85, 109,
			86, 49, 45, 45, 45, 52, 52, 99, 67, 122, 57, 45, 45, 118, 56, 70,
			78, 48, 52, 55, 85, 101, 107, 56, 100, 56, 83, 51, 86, 52, 87, 52,
			104, 82, 0, 0, 253, 57, 243, 139, 140, 46, 42, 192, 0, 255, 0,
			255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 201, 0, 0},
		uint64(1398904314371213951),
	}
	c.Check(rows[1], DeepEquals, expected2)

	expected3 := RowValues{
		// 'L\x0a\x19??#$?\x00\x00\x00\x00\x00\x00\x00\x09'
		[]byte{76, 10, 25, 139, 200, 35, 36, 129, 0, 0, 0, 0, 0, 0, 0, 9},
		// '?9?.*?\x00?\x00?\x00?\x00?\x00?\x00?\x00?\x03\x00\x00\x01\x00\x00'
		// '\x01\x00\x00?\x00?\x00?\x00?\x00\x00V--0GPFmkPX---44d2A0e-v8FN04'
		// '7Uek8d8S3a82bxk\x00\x00?9?.*?\x00?\x00?\x00?\x00?\x00?\x00?\x00??'
		// '\x00\x00'
		[]byte{
			253, 57, 243, 139, 140, 46, 42, 192, 0, 255, 0, 255, 0, 255, 0,
			255, 0, 255, 0, 255, 0, 255, 3, 0, 0, 1, 0, 0, 1, 0, 0, 128, 0,
			255, 0, 255, 0, 255, 0, 0, 86, 45, 45, 48, 71, 80, 70, 109, 107,
			80, 88, 45, 45, 45, 52, 52, 100, 50, 65, 48, 101, 45, 118, 56,
			70, 78, 48, 52, 55, 85, 101, 107, 56, 100, 56, 83, 51, 97, 56,
			50, 98, 120, 107, 0, 0, 253, 57, 243, 139, 140, 46, 42, 192, 0,
			255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255, 200, 0, 0},
		uint64(1398904314371213950),
	}
	c.Check(rows[2], DeepEquals, expected3)
}
