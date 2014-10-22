package binlog

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type FormatDescriptionEventSuite struct {
	EventParserSuite
}

var _ = Suite(&FormatDescriptionEventSuite{})

func (s *FormatDescriptionEventSuite) Test55FDE(c *C) {
	// This FDE entry was copied from a 5.5 SFJ shard.
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
		uint16(0),
		[]byte{
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
			26, 8, 0, 0, 0, 8, 8, 8, 2, 0})

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)
	c.Check(fde.BinlogVersion(), Equals, uint16(4))
	c.Check(string(fde.ServerVersion()), Equals, "5.5.34-32.0-log")
	c.Check(fde.CreatedTimestamp(), Equals, uint32(0))
	c.Check(fde.ExtraHeadersSize(), Equals, 0)

	for i := 0; i < int(mysql_proto.LogEventType_HEARTBEAT_LOG_EVENT); i++ {
		c.Log(mysql_proto.LogEventType_Type(i).String())
		_, inMap := fde.fixedLengthSizes[mysql_proto.LogEventType_Type(i)]
		c.Check(inMap, IsTrue)
	}

	for i := int(mysql_proto.LogEventType_IGNORABLE_LOG_EVENT); i < int(mysql_proto.LogEventType_PREVIOUS_GTIDS_LOG_EVENT); i++ {

		c.Log(mysql_proto.LogEventType_Type(i).String())
		_, inMap := fde.fixedLengthSizes[mysql_proto.LogEventType_Type(i)]
		c.Check(inMap, IsFalse)
	}

	c.Check(fde.ChecksumAlgorithm(), Equals, mysql_proto.ChecksumAlgorithm_OFF)
	c.Check(fde.Checksum(), DeepEquals, []byte{})
}

func (s *FormatDescriptionEventSuite) Test56FDE(c *C) {
	// This FDE entry was copied from a 5.6 HDB shard.
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
		uint16(0),
		[]byte{
			// binlog version
			4, 0,
			// server version
			53, 46, 54, 46, 49, 53, 45, 54, 51, 46,
			48, 45, 108, 111, 103, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			// created timestamp
			0, 0, 0, 0,
			// total header size
			19,
			// fixed length data size per event type
			56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 92, 0, 4, 26,
			8, 0, 0, 0, 8, 8, 8, 2, 0, 0, 0, 10, 10, 10, 25, 25, 0,
			// checksum algorithm
			1,
			// checksum
			40, 216, 52, 169}) // 0x28 0xd8 0x34 0xa9

	event, err := s.NextEvent()
	c.Assert(err, IsNil)

	c.Assert(event, NotNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)
	c.Check(fde.BinlogVersion(), Equals, uint16(4))
	c.Check(string(fde.ServerVersion()), Equals, "5.6.15-63.0-log")
	c.Check(fde.CreatedTimestamp(), Equals, uint32(0))
	c.Check(fde.ExtraHeadersSize(), Equals, 0)

	for i := 0; i < int(mysql_proto.LogEventType_PREVIOUS_GTIDS_LOG_EVENT); i++ {
		c.Log(mysql_proto.LogEventType_Type(i).String())
		_, inMap := fde.fixedLengthSizes[mysql_proto.LogEventType_Type(i)]
		c.Check(inMap, IsTrue)
	}

	c.Check(fde.ChecksumAlgorithm(), Equals, mysql_proto.ChecksumAlgorithm_CRC32)
	c.Check(fde.Checksum(), DeepEquals, []byte{40, 216, 52, 169})

	// extra sanity checks
	c.Check(fde.SourceName(), Equals, testSourceName)
	c.Check(
		fde.EventType(),
		Equals,
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT)

	_, ok = event.(*RawV4Event)
	c.Check(ok, IsFalse)
}
