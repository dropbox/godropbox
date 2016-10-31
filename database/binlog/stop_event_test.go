package binlog

import (
	. "github.com/dropbox/godropbox/gocheck2"

	. "gopkg.in/check.v1"

	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type StopEventSuite struct {
	EventParserSuite
}

var _ = Suite(&StopEventSuite{})

func (s *StopEventSuite) TestStopEvent(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_STOP_EVENT,
		uint16(0),
		[]byte{})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	_, ok := event.(*StopEvent)
	c.Assert(ok, IsTrue)
}
