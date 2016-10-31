package binlog

import (
	"bytes"
	"encoding/binary"
	"io"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/errors"
	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type parsedTestEvent struct {
	Event
	value uint32
}

type testV4EventParser struct {
	hasNoTableContext

	eventType           mysql_proto.LogEventType_Type
	fixedLengthDataSize int
}

func newTestV4EventParser(
	eventType mysql_proto.LogEventType_Type,
	fixedLengthDataSize int) *testV4EventParser {

	return &testV4EventParser{
		eventType:           eventType,
		fixedLengthDataSize: fixedLengthDataSize,
	}
}

func (p *testV4EventParser) EventType() mysql_proto.LogEventType_Type {
	return p.eventType
}

func (p *testV4EventParser) FixedLengthDataSize() int {
	return p.fixedLengthDataSize
}

func (p *testV4EventParser) Parse(raw *RawV4Event) (Event, error) {
	event := &parsedTestEvent{
		Event: raw,
	}

	if len(raw.FixedLengthData()) != 4 {
		return raw, errors.New("Parse error")
	}
	err := binary.Read(
		bytes.NewReader(raw.FixedLengthData()),
		binary.BigEndian,
		&event.value)
	if err != nil {
		return raw, err
	}

	return event, nil
}

const testRegisteredEventType = mysql_proto.LogEventType_Type(1)
const testParseErrorEventType = mysql_proto.LogEventType_Type(2)
const testInvalidFixedLengthDataSizeEventType = mysql_proto.LogEventType_Type(3)
const testNotRegisteredEventType = mysql_proto.LogEventType_Type(4)
const testFDEEventType = mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT // 15

type ParsedV4EventReaderSuite struct {
	src       *bytes.Buffer
	rawReader EventReader
	parsers   V4EventParserMap
	reader    EventReader
}

var _ = Suite(&ParsedV4EventReaderSuite{})

func (s *ParsedV4EventReaderSuite) SetUpTest(c *C) {
	s.src = &bytes.Buffer{}
	s.rawReader = NewRawV4EventReader(s.src, testSourceName)

	parsers := &v4EventParserMap{
		extraHeadersSize:       8,
		parsers:                make(map[mysql_proto.LogEventType_Type]V4EventParser),
		numSupportedEventTypes: 16,
	}

	parsers.set(newTestV4EventParser(testRegisteredEventType, 4))
	parsers.set(newTestV4EventParser(testParseErrorEventType, 0))
	parsers.set(
		NewNoOpV4EventParser(testInvalidFixedLengthDataSizeEventType, 1000))
	parsers.set(NewNoOpV4EventParser(testFDEEventType, 3))

	s.parsers = parsers
	s.reader = NewParsedV4EventReader(s.rawReader, parsers)
}

func (s *ParsedV4EventReaderSuite) WriteEvent(
	eventType mysql_proto.LogEventType_Type,
	data []byte) {

	eventBytes, err := CreateEventBytes(
		uint32(0), // timestamp
		uint8(eventType),
		uint32(1), // server id
		uint32(3), // next position
		uint16(4), // flags
		data)
	if err != nil {
		panic(err)
	}

	_, err = s.src.Write(eventBytes)
	if err != nil {
		panic(err)
	}
}

func (s *ParsedV4EventReaderSuite) TestFDESetExtraHeadersSize(c *C) {
	s.parsers.SetChecksumSize(1)

	s.WriteEvent(testFDEEventType, []byte("foo bar"))

	event, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	c.Check(event.ExtraHeaders(), DeepEquals, []byte{})
	c.Check(event.FixedLengthData(), DeepEquals, []byte("foo"))
	c.Check(event.VariableLengthData(), DeepEquals, []byte(" bar"))
	c.Check(event.Checksum(), DeepEquals, []byte{})
}

func (s *ParsedV4EventReaderSuite) TestNoRegisteredParser(c *C) {
	s.WriteEvent(testNotRegisteredEventType, []byte("foo bar hello world"))

	event, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.ExtraHeaders(), DeepEquals, []byte("foo bar "))
	c.Check(event.FixedLengthData(), DeepEquals, []byte{})
	c.Check(event.VariableLengthData(), DeepEquals, []byte("hello world"))
}

func (s *ParsedV4EventReaderSuite) TestParse(c *C) {
	s.WriteEvent(
		testRegisteredEventType,
		[]byte("header  \xde\xca\xfb\xadrest of the body"))

	event, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*RawV4Event)
	c.Check(ok, IsFalse)
	e, ok := event.(*parsedTestEvent)
	c.Check(ok, IsTrue)
	c.Check(event.ExtraHeaders(), DeepEquals, []byte("header  "))
	c.Check(event.FixedLengthData(), DeepEquals, []byte("\xde\xca\xfb\xad"))
	c.Check(e.value, Equals, uint32(0xdecafbad))
	c.Check(event.VariableLengthData(), DeepEquals, []byte("rest of the body"))
	c.Check(event.Checksum(), DeepEquals, []byte{})
}

func (s *ParsedV4EventReaderSuite) TestNonFDEChecksum(c *C) {
	s.parsers.SetChecksumSize(8)

	s.WriteEvent(
		testRegisteredEventType,
		[]byte("header  \xde\xca\xfb\xadrest of the bodychecksum"))

	event, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*RawV4Event)
	c.Check(ok, IsFalse)
	e, ok := event.(*parsedTestEvent)
	c.Check(ok, IsTrue)
	c.Check(event.ExtraHeaders(), DeepEquals, []byte("header  "))
	c.Check(event.FixedLengthData(), DeepEquals, []byte("\xde\xca\xfb\xad"))
	c.Check(e.value, Equals, uint32(0xdecafbad))
	c.Check(event.VariableLengthData(), DeepEquals, []byte("rest of the body"))
	c.Check(event.Checksum(), DeepEquals, []byte("checksum"))
}

func (s *ParsedV4EventReaderSuite) TestInvalidSetExtraHeadersSize(c *C) {
	s.WriteEvent(testRegisteredEventType, []byte("short"))

	event, err := s.reader.NextEvent()

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid extra headers size"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	_, ok := event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.ExtraHeaders(), DeepEquals, []byte{})
	c.Check(event.FixedLengthData(), DeepEquals, []byte{})
	c.Check(event.VariableLengthData(), DeepEquals, []byte("short"))
}

func (s *ParsedV4EventReaderSuite) TestInvalidSetFixedLengthDataSize(c *C) {
	s.WriteEvent(
		testInvalidFixedLengthDataSizeEventType,
		[]byte("_header_still too short"))

	event, err := s.reader.NextEvent()

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid fixed length data's size"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	_, ok := event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.ExtraHeaders(), DeepEquals, []byte("_header_"))
	c.Check(event.FixedLengthData(), DeepEquals, []byte{})
	c.Check(event.VariableLengthData(), DeepEquals, []byte("still too short"))
}

func (s *ParsedV4EventReaderSuite) TestParseError(c *C) {
	s.WriteEvent(
		testParseErrorEventType,
		[]byte("_header_rest of the body"))

	event, err := s.reader.NextEvent()
	c.Log(event)

	_, ok := event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.ExtraHeaders(), DeepEquals, []byte("_header_"))
	c.Check(event.FixedLengthData(), DeepEquals, []byte(""))
	c.Check(event.VariableLengthData(), DeepEquals, []byte("rest of the body"))

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Log(err)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Parse error"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

}

func (s *ParsedV4EventReaderSuite) TestReadStream(c *C) {

	s.WriteEvent(testFDEEventType, []byte("foo bar"))

	s.WriteEvent(
		testRegisteredEventType,
		[]byte("header  \xde\xca\xfb\xadrest of the body"))

	s.WriteEvent(testRegisteredEventType, []byte("short"))

	s.WriteEvent(
		testRegisteredEventType,
		[]byte("header  \x01\x02\x03\x04msg 2"))

	s.WriteEvent(
		testInvalidFixedLengthDataSizeEventType,
		[]byte("_header_still too short"))

	s.WriteEvent(
		testParseErrorEventType,
		[]byte("_header_rest of the body"))

	s.WriteEvent(
		testRegisteredEventType,
		[]byte("header  \x01\x02\x03\x04message 3"))

	s.WriteEvent(testFDEEventType, []byte("_HEADER_"))

	event, err := s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testFDEEventType)

	event, err = s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*parsedTestEvent)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testRegisteredEventType)

	event, err = s.reader.NextEvent()
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	_, ok = event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testRegisteredEventType)

	event, err = s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*parsedTestEvent)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testRegisteredEventType)

	event, err = s.reader.NextEvent()
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	_, ok = event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testInvalidFixedLengthDataSizeEventType)

	event, err = s.reader.NextEvent()
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	_, ok = event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testParseErrorEventType)

	event, err = s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*parsedTestEvent)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testRegisteredEventType)

	event, err = s.reader.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*RawV4Event)
	c.Check(ok, IsTrue)
	c.Check(event.EventType(), Equals, testFDEEventType)

	event, err = s.reader.NextEvent()
	c.Check(event, IsNil)
	c.Check(err, Equals, io.EOF)
}
