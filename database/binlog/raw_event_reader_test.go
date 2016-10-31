package binlog

import (
	"bytes"
	"io"

	. "gopkg.in/check.v1"

	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type RawV4EventReaderSuite struct {
	src    *bytes.Buffer
	reader EventReader
}

var _ = Suite(&RawV4EventReaderSuite{})

const testSourceName = "test_stream"

func (s *RawV4EventReaderSuite) SetUpTest(c *C) {
	s.src = &bytes.Buffer{}
	s.reader = NewRawV4EventReader(s.src, testSourceName)
}

func (s *RawV4EventReaderSuite) GenerateEvent(
	timestamp uint32,
	eventType uint8,
	serverId uint32,
	nextPosition uint32,
	flags uint16,
	dataLength int) []byte {

	data := ""
	for i := 0; i < dataLength; i++ {
		data += "\xfe"
	}

	result, err := CreateEventBytes(
		timestamp,
		eventType,
		serverId,
		nextPosition,
		flags,
		[]byte(data))

	if err != nil {
		panic(err)
	}

	return result
}

func (s *RawV4EventReaderSuite) TestParseSimpleEvents(c *C) {
	// hand code a single event just to be sure
	eventBytes1 := []byte(
		"\x04\x03\x02\x01" + // timestamp
			"\x12" + // event type
			"\x0f\x0e\x0e\x0b" + // server id
			"\x16\x00\x00\x00" + // event length (19 header + 3 data)
			"\xf4\xf3\xf2\xf1" + // next position
			"\xad\xde" + // flags
			"\x0a\x0b\x0c") // data

	_, err := s.src.Write(eventBytes1)
	c.Assert(err, IsNil)

	eventBytes2 := s.GenerateEvent(
		0xdeadbeef, // timestamp
		0x69,       // event type
		0xdecafbad, // server id
		0x12345678, // next position
		0xf00d,     // flags
		7)          // data length
	_, err = s.src.Write(eventBytes2)
	c.Assert(err, IsNil)

	eventBytes3 := s.GenerateEvent(
		0x1aaaaaaa, // timestamp
		0x2b,       // event type
		0x3ccccccc, // server id
		0x4ddddddd, // next position
		0x5eee,     // flags
		0)          // data length
	_, err = s.src.Write(eventBytes3)
	c.Assert(err, IsNil)

	event1, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	c.Check(event1.SourceName(), Equals, testSourceName)
	c.Check(event1.SourcePosition(), Equals, int64(0))
	c.Check(event1.Timestamp(), Equals, uint32(0x01020304))
	c.Check(event1.EventType(), Equals, mysql_proto.LogEventType_Type(0x12))
	c.Check(event1.ServerId(), Equals, uint32(0x0b0e0e0f))
	c.Check(event1.EventLength(), Equals, uint32(len(eventBytes1)))
	c.Check(event1.NextPosition(), Equals, uint32(0xf1f2f3f4))
	c.Check(event1.Flags(), Equals, uint16(0xdead))
	c.Check(event1.Bytes(), DeepEquals, eventBytes1)
	c.Check(
		event1.BasicHeader(),
		DeepEquals,
		eventBytes1[:sizeOfBasicV4EventHeader])
	c.Check(event1.ExtraHeaders(), DeepEquals, []byte{})
	c.Check(event1.FixedLengthData(), DeepEquals, []byte{})
	c.Check(event1.VariableLengthData(), DeepEquals, []byte("\x0a\x0b\x0c"))

	event2, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	c.Check(event2.SourceName(), Equals, testSourceName)
	c.Check(event2.SourcePosition(), Equals, int64(len(eventBytes1)))
	c.Check(event2.Timestamp(), Equals, uint32(0xdeadbeef))
	c.Check(event2.EventType(), Equals, mysql_proto.LogEventType_Type(0x69))
	c.Check(event2.ServerId(), Equals, uint32(0xdecafbad))
	c.Check(event2.EventLength(), Equals, uint32(len(eventBytes2)))
	c.Check(event2.NextPosition(), Equals, uint32(0x12345678))
	c.Check(event2.Flags(), Equals, uint16(0xf00d))
	c.Check(event2.ExtraHeaders(), DeepEquals, []byte{})
	c.Check(event2.FixedLengthData(), DeepEquals, []byte{})
	c.Check(
		event2.VariableLengthData(),
		DeepEquals,
		[]byte("\xfe\xfe\xfe\xfe\xfe\xfe\xfe"))

	event3, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	c.Check(event3.SourceName(), Equals, testSourceName)
	c.Check(
		event3.SourcePosition(),
		Equals,
		int64(len(eventBytes1)+len(eventBytes2)))
	c.Check(event3.Timestamp(), Equals, uint32(0x1aaaaaaa))
	c.Check(event3.EventType(), Equals, mysql_proto.LogEventType_Type(0x2b))
	c.Check(event3.ServerId(), Equals, uint32(0x3ccccccc))
	c.Check(event3.EventLength(), Equals, uint32(len(eventBytes3)))
	c.Check(event3.NextPosition(), Equals, uint32(0x4ddddddd))
	c.Check(event3.Flags(), Equals, uint16(0x5eee))
	c.Check(event3.ExtraHeaders(), DeepEquals, []byte{})
	c.Check(event3.FixedLengthData(), DeepEquals, []byte{})
	c.Check(event3.VariableLengthData(), DeepEquals, []byte{})

	_, err = s.src.ReadByte()
	c.Assert(err, Equals, io.EOF)

	event4, err := s.reader.NextEvent()
	c.Check(event4, IsNil)
	c.Check(err, Equals, io.EOF)
}

func (s *RawV4EventReaderSuite) TestEOFMidHeader(c *C) {
	eventBytes := s.GenerateEvent(
		1, // timestamp
		2, // event type
		3, // server id
		4, // next position
		5, // flags
		2) // data length

	_, err := s.src.Write(eventBytes[:5])
	c.Assert(err, IsNil)

	event, err := s.reader.NextEvent()
	c.Assert(err, Equals, io.EOF)
	c.Check(event, IsNil)

	_, err = s.src.Write(eventBytes[5:13])
	c.Assert(err, IsNil)

	event, err = s.reader.NextEvent()
	c.Assert(err, Equals, io.EOF)
	c.Check(event, IsNil)
	c.Check(s.reader.nextEventEndPosition(), Equals, int64(19))

	// Finish writing the entire event.
	_, err = s.src.Write(eventBytes[13:])
	c.Assert(err, IsNil)

	event, err = s.reader.NextEvent()
	c.Assert(err, IsNil)
	c.Check(event.SourceName(), Equals, testSourceName)
	c.Check(event.SourcePosition(), Equals, int64(0))
	c.Check(event.Timestamp(), Equals, uint32(1))
	c.Check(event.EventType(), Equals, mysql_proto.LogEventType_Type(2))
	c.Check(event.ServerId(), Equals, uint32(3))
	c.Check(event.EventLength(), Equals, uint32(len(eventBytes)))
	c.Check(event.NextPosition(), Equals, uint32(4))
	c.Check(event.Flags(), Equals, uint16(5))
	c.Check(event.ExtraHeaders(), DeepEquals, []byte{})
	c.Check(event.FixedLengthData(), DeepEquals, []byte{})
	c.Check(event.VariableLengthData(), DeepEquals, []byte("\xfe\xfe"))

	c.Check(
		s.reader.nextEventEndPosition(),
		Equals,
		int64(event.EventLength())+int64(19))
}

func (s *RawV4EventReaderSuite) TestEOFMidBody(c *C) {
	eventBytes := s.GenerateEvent(
		1,  // timestamp
		2,  // event type
		3,  // server id
		4,  // next position
		5,  // flags
		10) // data length

	written := 0

	_, err := s.src.Write(eventBytes[:sizeOfBasicV4EventHeader])
	c.Assert(err, IsNil)
	written += sizeOfBasicV4EventHeader

	event, err := s.reader.NextEvent()
	c.Assert(err, Equals, io.EOF)
	c.Check(event, IsNil)
	c.Check(s.reader.nextEventEndPosition(), Equals, int64(len(eventBytes)))

	_, err = s.src.Write(eventBytes[written : written+3])
	c.Assert(err, IsNil)
	written += 3
	c.Check(s.reader.nextEventEndPosition(), Equals, int64(len(eventBytes)))

	event, err = s.reader.NextEvent()
	c.Assert(err, Equals, io.EOF)
	c.Check(event, IsNil)

	_, err = s.src.Write(eventBytes[written : written+4])
	c.Assert(err, IsNil)
	written += 4

	event, err = s.reader.NextEvent()
	c.Assert(err, Equals, io.EOF)
	c.Check(event, IsNil)

	// Finish writing the entire event.
	_, err = s.src.Write(eventBytes[written:])
	c.Assert(err, IsNil)

	event, err = s.reader.NextEvent()
	c.Assert(err, IsNil)
	c.Check(event.SourceName(), Equals, testSourceName)
	c.Check(event.SourcePosition(), Equals, int64(0))
	c.Check(event.Timestamp(), Equals, uint32(1))
	c.Check(event.EventType(), Equals, mysql_proto.LogEventType_Type(2))
	c.Check(event.ServerId(), Equals, uint32(3))
	c.Check(event.EventLength(), Equals, uint32(len(eventBytes)))
	c.Check(event.NextPosition(), Equals, uint32(4))
	c.Check(event.Flags(), Equals, uint16(5))
	c.Check(event.ExtraHeaders(), DeepEquals, []byte{})
	c.Check(event.FixedLengthData(), DeepEquals, []byte{})
	c.Check(
		event.VariableLengthData(),
		DeepEquals,
		[]byte("\xfe\xfe\xfe\xfe\xfe\xfe\xfe\xfe\xfe\xfe"))
	c.Check(
		s.reader.nextEventEndPosition(),
		Equals,
		int64(len(eventBytes))+int64(19))

	_, err = s.src.ReadByte()
	c.Assert(err, Equals, io.EOF)

	event, err = s.reader.NextEvent()
	c.Check(event, IsNil)
	c.Check(err, Equals, io.EOF)
}

func (s *RawV4EventReaderSuite) TestInvalidNegBodyLength(c *C) {
	eventBytes := []byte(
		"\x04\x03\x02\x01" + // timestamp
			"\x12" + // event type
			"\x0f\x0e\x0e\x0b" + // server id
			"\x05\x00\x00\x00" + // invalid event length
			"\xf4\xf3\xf2\xf1" + // next position
			"\xad\xde" + // flags
			"\x0a\x0b\x0c") // data

	_, err := s.src.Write(eventBytes)
	c.Assert(err, IsNil)

	event, err := s.reader.NextEvent()
	c.Check(event, IsNil)

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid event size"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	// Gave up reading after the header
	c.Check(s.src.Bytes(), DeepEquals, []byte("\x0a\x0b\x0c"))
}

func (s *RawV4EventReaderSuite) TestPeekHeaderBytes(c *C) {
	eventBytes := []byte(
		"\x04\x03\x02\x01" + // timestamp
			"\x12" + // event type
			"\x0f\x0e\x0e\x0b" + // server id
			"\x16\x00\x00\x00" + // event length (19 header + 3 data)
			"\xf4\xf3\xf2\xf1" + // next position
			"\xad\xde" + // flags
			"\x0a\x0b\x0c") // data

	_, err := s.src.Write(eventBytes)
	c.Assert(err, IsNil)

	result, err := s.reader.peekHeaderBytes(2)
	c.Check(err, IsNil)
	c.Check(result, DeepEquals, eventBytes[:2])

	result, err = s.reader.peekHeaderBytes(6)
	c.Check(err, IsNil)
	c.Check(result, DeepEquals, eventBytes[:6])

	result, err = s.reader.peekHeaderBytes(sizeOfBasicV4EventHeader)
	c.Check(err, IsNil)
	c.Check(result, DeepEquals, eventBytes[:sizeOfBasicV4EventHeader])
}

func (s *RawV4EventReaderSuite) TestPeekHeaderBytesTooFarAhead(c *C) {
	eventBytes := []byte(
		"\x04\x03\x02\x01" + // timestamp
			"\x12" + // event type
			"\x0f\x0e\x0e\x0b" + // server id
			"\x16\x00\x00\x00" + // event length (19 header + 3 data)
			"\xf4\xf3\xf2\xf1" + // next position
			"\xad\xde" + // flags
			"\x0a\x0b\x0c") // data

	_, err := s.src.Write(eventBytes)
	c.Assert(err, IsNil)

	result, err := s.reader.peekHeaderBytes(sizeOfBasicV4EventHeader + 1)
	c.Check(result, IsNil)

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Buffer full"
	c.Assert(err.Error()[:len(expected)], Equals, expected)
}

func (s *RawV4EventReaderSuite) TestConsumeHeaderBytes(c *C) {
	c.Assert(len(logFileMagic), Not(Equals), 0)

	_, err := s.src.Write(logFileMagic)
	c.Assert(err, IsNil)

	eventBytes := s.GenerateEvent(
		1,  // timestamp
		2,  // event type
		3,  // server id
		4,  // next position
		5,  // flags
		10) // data length

	_, err = s.src.Write(eventBytes)
	c.Assert(err, IsNil)

	result, err := s.reader.peekHeaderBytes(len(logFileMagic))
	c.Assert(err, IsNil)
	c.Check(result, DeepEquals, logFileMagic)

	err = s.reader.consumeHeaderBytes(len(logFileMagic))
	c.Assert(err, IsNil)

	event, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	c.Check(event.Timestamp(), Equals, uint32(1))
}

func (s *RawV4EventReaderSuite) TestConsumeHeaderBytesWithoutPeeking(c *C) {
	eventBytes := s.GenerateEvent(
		1,  // timestamp
		2,  // event type
		3,  // server id
		4,  // next position
		5,  // flags
		10) // data length

	_, err := s.src.Write(eventBytes[:sizeOfBasicV4EventHeader+5])
	c.Assert(err, IsNil)

	err = s.reader.consumeHeaderBytes(2)
	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Consuming more bytes than"
	c.Assert(err.Error()[:len(expected)], Equals, expected)
}

func (s *RawV4EventReaderSuite) TestInvalidEOFMidBodyConsumeHeaderBytes(c *C) {
	eventBytes := s.GenerateEvent(
		1,  // timestamp
		2,  // event type
		3,  // server id
		4,  // next position
		5,  // flags
		10) // data length

	_, err := s.src.Write(eventBytes[:sizeOfBasicV4EventHeader+5])
	c.Assert(err, IsNil)

	_, err = s.reader.NextEvent()
	c.Assert(err, Equals, io.EOF)

	err = s.reader.consumeHeaderBytes(2)
	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Cannot consume header bytes"
	c.Assert(err.Error()[:len(expected)], Equals, expected)
}
