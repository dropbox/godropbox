package binlog

import (
	"bytes"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
	. "gopkg.in/check.v1"
)

type GtidLogEventSuite struct {
	EventParserSuite
}

var _ = Suite(&GtidLogEventSuite{})

func (s *GtidLogEventSuite) TestCommitGtid(c *C) {
	data := &bytes.Buffer{}
	// commit
	data.WriteByte(1)
	// sid
	sid := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	data.Write(sid)
	// gno
	data.Write([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	s.WriteEvent(
		mysql_proto.LogEventType_GTID_LOG_EVENT,
		uint16(0),
		data.Bytes())

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	gle, ok := event.(*GtidLogEvent)
	c.Assert(ok, IsTrue)
	c.Assert(gle.IsCommit(), IsTrue)
	for i, sidByte := range sid {
		c.Assert(gle.Sid()[i], Equals, sidByte)
	}
	c.Assert(gle.Gno(), Equals, uint64(1))
}

func (s *GtidLogEventSuite) TestBadCommitGid(c *C) {
	data := &bytes.Buffer{}
	// commit
	data.WriteByte(9)
	// sid
	sid := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	data.Write(sid)
	// gno
	data.Write([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	s.WriteEvent(
		mysql_proto.LogEventType_GTID_LOG_EVENT,
		uint16(0),
		data.Bytes())

	_, err := s.NextEvent()
	c.Assert(err, NotNil)
}

func (s *GtidLogEventSuite) TestShortHeader(c *C) {
	data := &bytes.Buffer{}
	// commit
	data.WriteByte(1)
	data.Write([]byte{0, 1, 2, 3, 4})
	s.WriteEvent(
		mysql_proto.LogEventType_GTID_LOG_EVENT,
		uint16(0),
		data.Bytes())

	_, err := s.NextEvent()
	c.Assert(err, NotNil)
}

func (s *GtidLogEventSuite) TestLongHeader(c *C) {
	data := &bytes.Buffer{}
	// commit
	data.WriteByte(1)
	data.Write([]byte{
		0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
		0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
		0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
		0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
		0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
	})
	s.WriteEvent(
		mysql_proto.LogEventType_GTID_LOG_EVENT,
		uint16(0),
		data.Bytes())

	_, err := s.NextEvent()
	c.Assert(err, NotNil)
}
