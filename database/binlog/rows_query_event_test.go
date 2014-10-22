package binlog

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type RowsQueryEventSuite struct {
	EventParserSuite
}

var _ = Suite(&RowsQueryEventSuite{})

func (s *RowsQueryEventSuite) TestBasic(c *C) {
	s.WriteEvent(
		mysql_proto.LogEventType_ROWS_QUERY_LOG_EVENT,
		uint16(0),
		[]byte{
			51, 105, 110, 115, 101, 114, 116, 32, 105, 110, 116,
			111, 32, 98, 108, 97, 104, 32, 118, 97, 108, 117, 101,
			115, 32, 40, 52, 52, 52, 49, 48, 49, 48, 41, 32, 47, 42,
			112, 97, 114, 116, 32, 111, 102, 32, 113, 117, 101, 114,
			121, 42, 47,
		})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	q, ok := event.(*RowsQueryEvent)
	c.Assert(ok, IsTrue)
	c.Check(
		q.TruncatedQuery(),
		DeepEquals,
		[]byte("insert into blah values (4441010) /*part of query*/"))
}
