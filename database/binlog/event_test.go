package binlog

import (
	"bytes"
	"testing"

	. "gopkg.in/check.v1"

	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// Hook up gocheck into the go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type EventParserSuite struct {
	src       *bytes.Buffer
	parsers   V4EventParserMap
	rawReader EventReader
	reader    EventReader
}

func (s *EventParserSuite) SetUpTest(c *C) {
	s.src = &bytes.Buffer{}
	s.parsers = NewV4EventParserMap()
	s.rawReader = NewRawV4EventReader(s.src, testSourceName)
	s.reader = NewParsedV4EventReader(s.rawReader, s.parsers)
}

func (s *EventParserSuite) Write(payload []byte) {
	_, err := s.src.Write(payload)
	if err != nil {
		panic(err)
	}
}

func (s *EventParserSuite) WriteEvent(
	eventType mysql_proto.LogEventType_Type,
	flags uint16,
	data []byte) {

	eventBytes, err := CreateEventBytes(
		uint32(0), // timestamp
		uint8(eventType),
		uint32(1),    // server id
		uint32(1234), // next position
		flags,
		data)
	if err != nil {
		panic(err)
	}

	s.Write(eventBytes)
}

func (s *EventParserSuite) NextEvent() (Event, error) {
	return s.reader.NextEvent()
}

func (s *EventParserSuite) SetChecksumSize(size int) {
	s.parsers.SetChecksumSize(size)
}
