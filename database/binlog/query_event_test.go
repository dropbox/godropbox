package binlog

import (
	"bytes"
	"encoding/binary"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type QueryEventSuite struct {
	EventParserSuite
}

var _ = Suite(&QueryEventSuite{})

func (s *QueryEventSuite) WriteEventStatus(status []byte) {
	msg := []byte{
		// thread id
		192, 226, 18, 0,
		// duration
		0, 0, 0, 0,
		// db name length
		18,
		// error code
		0, 0}

	writer := &bytes.Buffer{}
	binary.Write(writer, binary.LittleEndian, uint16(len(status)))
	msg = append(msg, writer.Bytes()...)

	msg = append(msg, status...)

	msg = append(
		msg,
		[]byte{
			// db name
			105, 110, 102, 111, 114, 109, 97, 116, 105, 111,
			110, 95, 115, 99, 104, 101, 109, 97, 0,
			// query
			66, 69, 71, 73, 78}...)

	s.WriteEvent(
		mysql_proto.LogEventType_QUERY_EVENT,
		uint16(0),
		msg)
}

func (s *QueryEventSuite) TestDurationErrorCode(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_QUERY_EVENT,
		uint16(0),
		[]byte{
			// thread id
			179, 208, 22, 0,
			// duration
			1, 2, 3, 4,
			// db name length
			6,
			// error code (ER_DISK_FULL = 1021)
			0xfd, 0x03,
			// status length
			26, 0,
			// status block
			0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
			0, 0, 0, 0, 6, 3, 115, 116, 100, 4,
			33, 0, 33, 0, 192, 0,
			// db name
			103, 108, 111, 98, 97, 108, 0,
			// query
			66, 69, 71, 73, 78})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	q, ok := event.(*QueryEvent)
	c.Assert(ok, IsTrue)
	c.Check(q.Duration(), Equals, uint32(0x04030201))
	c.Check(q.ErrorCode(), Equals, mysql_proto.ErrorCode_ER_DISK_FULL)
}

func (s *QueryEventSuite) Test55Query(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_QUERY_EVENT,
		uint16(0),
		[]byte{
			// thread id
			179, 208, 22, 0,
			// duration
			0, 0, 0, 0,
			// db name length
			6,
			// error code
			0, 0,
			// status length
			26, 0,
			// status block
			0, 0, 0, 0, 0,
			1, 0, 0, 0, 0, 0, 0, 0, 0,
			6, 3, 115, 116, 100,
			4, 33, 0, 33, 0, 192, 0,
			// db name
			103, 108, 111, 98, 97, 108, 0,
			// query
			66, 69, 71, 73, 78})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	q, ok := event.(*QueryEvent)
	c.Assert(ok, IsTrue)
	c.Check(q.ThreadId(), Equals, uint32(0x0016d0b3))
	c.Check(q.Duration(), Equals, uint32(0))
	c.Check(q.ErrorCode(), Equals, mysql_proto.ErrorCode_OK)
	c.Check(
		q.StatusBytes(),
		DeepEquals,
		[]byte{
			0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
			0, 0, 0, 0, 6, 3, 115, 116, 100, 4,
			33, 0, 33, 0, 192, 0})
	// TODO check parsed status
	c.Check(string(q.DatabaseName()), Equals, "global")
	c.Check(string(q.Query()), Equals, "BEGIN")

	c.Assert(q.Flags2(), NotNil)
	c.Check(*q.Flags2(), Equals, uint32(0))
	c.Assert(q.SqlMode(), NotNil)
	c.Check(*q.SqlMode(), Equals, uint64(0))
	c.Check(string(q.Catalog()), Equals, "std")
	c.Check(q.Charset(), DeepEquals, []byte{33, 0, 33, 0, 192, 0})
}

func (s *QueryEventSuite) Test56Query(c *C) {
	s.SetChecksumSize(4)

	s.WriteEvent(
		mysql_proto.LogEventType_QUERY_EVENT,
		uint16(0),
		[]byte{
			// thread id
			192, 226, 18, 0,
			// duration
			0, 0, 0, 0,
			// db name length
			18,
			// error code
			0, 0,
			// status length
			26, 0,
			// status block
			0, 0, 0, 0, 0,
			1, 0, 0, 224, 93, 0, 0, 0, 0,
			6, 3, 115, 116, 100,
			4, 33, 0, 33, 0, 192, 0,
			// db name
			105, 110, 102, 111, 114, 109, 97, 116, 105, 111,
			110, 95, 115, 99, 104, 101, 109, 97, 0,
			// query
			66, 69, 71, 73, 78,
			// checksum
			117, 55, 35, 139})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	q, ok := event.(*QueryEvent)
	c.Assert(ok, IsTrue)
	c.Check(q.ThreadId(), Equals, uint32(0x0012e2c0))
	c.Check(q.Duration(), Equals, uint32(0))
	c.Check(q.ErrorCode(), Equals, mysql_proto.ErrorCode_OK)
	c.Check(
		q.StatusBytes(),
		DeepEquals,
		[]byte{
			0, 0, 0, 0, 0, 1, 0, 0, 224, 93,
			0, 0, 0, 0, 6, 3, 115, 116, 100, 4,
			33, 0, 33, 0, 192, 0})
	// TODO check parsed status
	c.Check(string(q.DatabaseName()), Equals, "information_schema")
	c.Check(string(q.Query()), Equals, "BEGIN")

	c.Assert(q.Flags2(), NotNil)
	c.Check(*q.Flags2(), Equals, uint32(0))
	c.Assert(q.SqlMode(), NotNil)
	c.Check(*q.SqlMode(), Equals, uint64(0x5de00000))
	c.Check(string(q.Catalog()), Equals, "std")
	c.Check(q.Charset(), DeepEquals, []byte{33, 0, 33, 0, 192, 0})

	c.Check(
		q.IsModeEnabled(mysql_proto.SqlMode_ERROR_FOR_DIVISION_BY_ZERO),
		Equals,
		true)
	c.Check(
		q.IsModeEnabled(mysql_proto.SqlMode_NO_ENGINE_SUBSTITUTION),
		Equals,
		true)
	c.Check(
		q.IsModeEnabled(mysql_proto.SqlMode_TRADITIONAL),
		Equals,
		true)
	c.Check(
		q.IsModeEnabled(mysql_proto.SqlMode_REAL_AS_FLOAT),
		Equals,
		false)
}

func (s *QueryEventSuite) TestStatus(c *C) {
	s.WriteEventStatus([]byte{
		// flags2
		0, 1, 0, 0, 0,
		// sql mode
		1, 2, 0, 0, 0, 0, 0, 0, 0,
		// catalog
		6, 3, 's', 't', 'd',
		// auto inc
		3, 3, 0, 4, 0,
		// charset
		4, 'd', 'e', 'c', 'a', 'f', 's',
		// time zone
		5, 4, 'a', 's', 'd', 'f',
		// lc time
		7, 5, 0,
		// charset database
		8, 6, 0,
		// table map for update
		9, 7, 0, 0, 0, 0, 0, 0, 0,
		// invoker
		11, 3, 'f', 'o', 'o', 4, 'b', 'a', 'r', 'z',
		// updated db name
		12, 254,
		// microseonds
		13, 9, 0, 0})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	q, ok := event.(*QueryEvent)
	c.Assert(ok, IsTrue)

	c.Assert(q.Flags2(), NotNil)
	c.Assert(q.SqlMode(), NotNil)
	c.Assert(q.Catalog(), NotNil)
	c.Assert(q.AutoIncIncrement(), NotNil)
	c.Assert(q.AutoIncOffset(), NotNil)
	c.Assert(q.Charset(), NotNil)
	c.Assert(q.TimeZone(), NotNil)
	c.Assert(q.LcTimeNamesNumber(), NotNil)
	c.Assert(q.CharsetDatabaseNumber(), NotNil)
	c.Assert(q.TableMapForUpdate(), NotNil)
	c.Assert(q.InvokerUser(), NotNil)
	c.Assert(q.InvokerHost(), NotNil)
	c.Assert(q.NumUpdatedDbs(), NotNil)
	c.Assert(q.Microseconds(), NotNil)

	c.Check(q.UpdatedDbNames(), IsNil)

	c.Check(*q.Flags2(), Equals, uint32(1))
	c.Check(*q.SqlMode(), Equals, uint64(2))
	c.Check(string(q.Catalog()), Equals, "std")
	c.Check(*q.AutoIncIncrement(), Equals, uint16(3))
	c.Check(*q.AutoIncOffset(), Equals, uint16(4))
	c.Check(string(q.Charset()), Equals, "decafs")
	c.Check(string(q.TimeZone()), Equals, "asdf")
	c.Check(*q.LcTimeNamesNumber(), Equals, uint16(5))
	c.Check(*q.CharsetDatabaseNumber(), Equals, uint16(6))
	c.Check(*q.TableMapForUpdate(), Equals, uint64(7))
	c.Check(string(q.InvokerUser()), Equals, "foo")
	c.Check(string(q.InvokerHost()), Equals, "barz")
	c.Check(*q.NumUpdatedDbs(), Equals, uint8(254))
	c.Check(*q.Microseconds(), Equals, uint32(9))
}

func (s *QueryEventSuite) TestUpdatedDbNamesStatus(c *C) {
	s.WriteEventStatus([]byte{
		12,
		4,
		'd', 'a', 't', 'a', 'b', 'a', 's', 'e', 0,
		'f', 'o', 'o', 0,
		'a', 's', 'd', 'f', 0,
		'z', 'z', 'z', 0,
	})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	q, ok := event.(*QueryEvent)
	c.Assert(ok, IsTrue)

	c.Assert(q.NumUpdatedDbs(), NotNil)
	c.Assert(q.UpdatedDbNames(), NotNil)

	c.Check(*q.NumUpdatedDbs(), Equals, uint8(4))
	c.Assert(len(q.UpdatedDbNames()), Equals, 4)
	c.Check(string(q.UpdatedDbNames()[0]), Equals, "database")
	c.Check(string(q.UpdatedDbNames()[1]), Equals, "foo")
	c.Check(string(q.UpdatedDbNames()[2]), Equals, "asdf")
	c.Check(string(q.UpdatedDbNames()[3]), Equals, "zzz")
}
