package binlog

import (
	. "godropbox/gocheck2"

	. "gopkg.in/check.v1"

	mysql_proto "dropbox/proto/mysql"
)

type HeartbeatEventSuite struct {
	EventParserSuite
}

var _ = Suite(&HeartbeatEventSuite{})

func (s *HeartbeatEventSuite) TestHeartbeatEvent(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_HEARTBEAT_LOG_EVENT,
		uint16(0),
		[]byte{})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	event, ok := event.(*HeartbeatEvent)
	c.Assert(ok, IsTrue)
	c.Assert(event.EventType(), Equals, mysql_proto.LogEventType_HEARTBEAT_LOG_EVENT)
}
