package binlog

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type XidEventSuite struct {
	EventParserSuite
}

var _ = Suite(&XidEventSuite{})

func (s *XidEventSuite) Test55Xid(c *C) {
	// From 5.5 SFJ relay log
	s.WriteEvent(
		mysql_proto.LogEventType_XID_EVENT,
		uint16(0),
		[]byte{117, 77, 99, 230, 0, 0, 0, 0})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	xe, ok := event.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(xe.Xid(), Equals, uint64(0x00000000e6634d75))
}

func (s *XidEventSuite) Test56Xid(c *C) {
	s.SetChecksumSize(4)

	// From 5.6 HDB relay log
	s.WriteEvent(
		mysql_proto.LogEventType_XID_EVENT,
		uint16(0),
		[]byte{
			// xid
			172, 224, 106, 186, 1, 0, 0, 0,
			// chekcsum
			23, 140, 1, 41})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	xe, ok := event.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(xe.Xid(), Equals, uint64(0x00000001ba6ae0ac))
}
