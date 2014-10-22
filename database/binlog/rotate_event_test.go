package binlog

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type RotateEventSuite struct {
	EventParserSuite
}

var _ = Suite(&RotateEventSuite{})

func (s *RotateEventSuite) Test55Rotate(c *C) {
	// This entry was copied from a 5.5 SFJ relay log
	s.WriteEvent(
		mysql_proto.LogEventType_ROTATE_EVENT,
		uint16(0),
		[]byte{
			// new log position
			4, 0, 0, 0, 0, 0, 0, 0,
			// new log name
			109, 121, 115, 113, 108, 100, 45, 114, 101, 108, 97, 121, 45,
			98, 105, 110, 46, 48, 48, 48, 55, 52, 57})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	rotate, ok := event.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(rotate.NewPosition(), Equals, uint64(4))
	logName := "mysqld-relay-bin.000749"
	c.Check(rotate.NewLogName(), DeepEquals, []byte(logName))
}

func (s *RotateEventSuite) Test56Rotate(c *C) {
	s.SetChecksumSize(4)

	// This entry was copied from a 5.6 HDB relay log
	s.WriteEvent(
		mysql_proto.LogEventType_ROTATE_EVENT,
		uint16(0),
		[]byte{
			// new log position
			4, 0, 0, 0, 0, 0, 0, 0,
			// new log name
			109, 121, 115, 113, 108, 100, 45, 114, 101, 108, 97, 121, 45,
			98, 105, 110, 46, 48, 48, 48, 48, 50, 49,
			// checksum
			231, 190, 239, 27})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	c.Log(event)
	rotate, ok := event.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(rotate.NewPosition(), Equals, uint64(4))
	logName := "mysqld-relay-bin.000021"
	c.Check(rotate.NewLogName(), DeepEquals, []byte(logName))
}
