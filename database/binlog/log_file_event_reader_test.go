package binlog

import (
	"bytes"
	"io"
	"log"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type LogFileV4EventReaderSuite struct {
	src        *bytes.Buffer
	parsers    V4EventParserMap
	reader     EventReader
	checksumed bool
}

var _ = Suite(&LogFileV4EventReaderSuite{})

func (s *LogFileV4EventReaderSuite) SetUpTest(c *C) {
	s.src = &bytes.Buffer{}
	s.parsers = NewV4EventParserMap()
	s.reader = NewLogFileV4EventReader(
		s.src,
		testSourceName,
		s.parsers,
		Logger{
			Fatalf:       log.Fatalf,
			Infof:        log.Printf,
			VerboseInfof: log.Printf,
		})
	s.checksumed = false
}

func (s *LogFileV4EventReaderSuite) NextEvent() (Event, error) {
	return s.reader.NextEvent()
}

func (s *LogFileV4EventReaderSuite) Write(b []byte) {
	_, err := s.src.Write(b)
	if err != nil {
		panic(err)
	}
}

func (s *LogFileV4EventReaderSuite) WriteLogFileMagic() {
	s.Write(logFileMagic)
}

func (s *LogFileV4EventReaderSuite) WriteEvent(
	eventType mysql_proto.LogEventType_Type,
	data []byte) {

	var t []byte
	if !s.checksumed ||
		eventType == mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT {

		t = data
	} else {
		t = make([]byte, len(data)+4, len(data)+4)
		copy(t, data)
		copy(t[len(data):], []byte("asdf"))
	}

	eventBytes, err := CreateEventBytes(
		uint32(0), // timestamp
		uint8(eventType),
		uint32(1),    // server id
		uint32(1234), // next position
		uint16(0),
		t)
	if err != nil {
		panic(err)
	}

	s.Write(eventBytes)
}

func (s *LogFileV4EventReaderSuite) Write55FDE() {
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
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
}

func (s *LogFileV4EventReaderSuite) Write56FDE() {
	checksumByte := byte(0)
	if s.checksumed {
		checksumByte = byte(1)
	}
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
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
			checksumByte,
			// checksum
			0, 0, 0, 0})
}

func (s *LogFileV4EventReaderSuite) Write55Master56FDE() {
	checksumByte := byte(0)
	if s.checksumed {
		checksumByte = byte(1)
	}
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
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
			8, 0, 0, 0, 8, 8, 8, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			// checksum algorithm
			checksumByte,
			// checksum
			0, 0, 0, 0})
}
func (s *LogFileV4EventReaderSuite) WriteXidEvent() {
	s.WriteEvent(
		mysql_proto.LogEventType_XID_EVENT,
		[]byte{117, 77, 99, 230, 0, 0, 0, 0})
}

func (s *LogFileV4EventReaderSuite) WriteRotateEvent() {
	s.WriteEvent(
		mysql_proto.LogEventType_ROTATE_EVENT,
		[]byte{
			// new log position
			4, 0, 0, 0, 0, 0, 0, 0,
			// new log name
			109, 121, 115, 113, 108, 100, 45, 114, 101, 108, 97, 121, 45,
			98, 105, 110, 46, 48, 48, 48, 55, 52, 57})
}

func (s *LogFileV4EventReaderSuite) TestFailedMagicCheck(c *C) {
	s.Write([]byte("asdf"))
	s.Write56FDE()

	event, err := s.NextEvent()

	c.Assert(event, IsNil)

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid binary log magic marker"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	header, err := s.reader.peekHeaderBytes(4)

	c.Assert(err, IsNil)
	c.Assert(header, DeepEquals, []byte("asdf"))
}

func (s *LogFileV4EventReaderSuite) TestV1FailFormatCheck(c *C) {
	s.WriteLogFileMagic()
	s.WriteEvent(
		mysql_proto.LogEventType_START_EVENT_V3,
		[]byte{})

	event, err := s.NextEvent()

	c.Assert(event, IsNil)

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Binary log reader does not support V1 binlog"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	header, err := s.reader.peekHeaderBytes(4)

	c.Assert(err, IsNil)
	c.Assert(header, DeepEquals, []byte{0, 0, 0, 0})
}

func (s *LogFileV4EventReaderSuite) TestImplicitV3FailFormatCheck(c *C) {
	s.WriteLogFileMagic()
	s.WriteXidEvent()

	event, err := s.NextEvent()

	c.Assert(event, IsNil)

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Binary log reader does not support V3 binlog"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	header, err := s.reader.peekHeaderBytes(4)

	c.Assert(err, IsNil)
	c.Assert(header, DeepEquals, []byte{0, 0, 0, 0})
}

func (s *LogFileV4EventReaderSuite) TestV3FailFormatCheck(c *C) {
	s.WriteLogFileMagic()
	s.WriteEvent(
		mysql_proto.LogEventType_START_EVENT_V3,
		[]byte{})

	event, err := s.NextEvent()

	c.Assert(event, IsNil)

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Binary log reader does not support V1 binlog"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	header, err := s.reader.peekHeaderBytes(4)

	c.Assert(err, IsNil)
	c.Assert(header, DeepEquals, []byte{0, 0, 0, 0})
}

func (s *LogFileV4EventReaderSuite) TestBadFDEFormatVersion(c *C) {
	s.WriteLogFileMagic()
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
		[]byte{
			// binlog version
			5, 0, // INVALID - should be 4, 0
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

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid binlog format version: 5"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	c.Assert(event, NotNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)
	c.Check(fde.BinlogVersion(), Equals, uint16(5))
}

func (s *LogFileV4EventReaderSuite) TestBadExtraHeadersSize(c *C) {
	s.WriteLogFileMagic()
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
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
			30, // INVALID - should be 19
			// fixed length data size per event type
			56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 84, 0, 4,
			26, 8, 0, 0, 0, 8, 8, 8, 2, 0})

	event, err := s.NextEvent()

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid extra headers size: 11"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	c.Assert(event, NotNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)
	c.Check(fde.ExtraHeadersSize(), Equals, 11)
}

func (s *LogFileV4EventReaderSuite) TestBadChecksumAlgorithm(c *C) {
	s.WriteLogFileMagic()
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
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
			255, // INVALID - should be 0 or 1
			// checksum
			0, 0, 0, 0})

	event, err := s.NextEvent()

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid checksum algorithm: 255 (UNDEFINED)"
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	c.Assert(event, NotNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)
	c.Check(
		fde.ChecksumAlgorithm(),
		Equals,
		mysql_proto.ChecksumAlgorithm_UNDEFINED)
}

func (s *LogFileV4EventReaderSuite) TestBadFixedLengthDataSizeForFDE(c *C) {
	s.WriteLogFileMagic()
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
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
			56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0,
			123, // INVALID - should be 84
			0, 4, 26, 8, 0, 0, 0, 8, 8, 8, 2, 0})

	event, err := s.NextEvent()

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid fixed length data size: "
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	c.Assert(event, NotNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)
	c.Check(
		fde.FixedLengthDataSizeForType(
			mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT),
		Equals,
		123)
}

func (s *LogFileV4EventReaderSuite) TestBadFixedLengthDataSizeForNonFDE(c *C) {
	s.WriteLogFileMagic()
	s.WriteEvent(
		mysql_proto.LogEventType_FORMAT_DESCRIPTION_EVENT,
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
			56, 13, 0,
			7, // INVALID - should be 8 (rotate)
			0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 84,
			1, // INVALID - should be 0 (xid)
			4, 26, 8, 0, 0, 0, 8, 8, 8, 2, 0})

	event, err := s.NextEvent()

	// gocheck.ErrorMatches does not seem to work correctly
	c.Assert(err, NotNil)
	c.Assert(err, Not(Equals), io.EOF)
	const expected = "Invalid fixed length data size: "
	c.Assert(err.Error()[:len(expected)], Equals, expected)

	c.Assert(event, NotNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)
	c.Check(
		fde.FixedLengthDataSizeForType(
			mysql_proto.LogEventType_ROTATE_EVENT),
		Equals,
		7)
	c.Check(
		fde.FixedLengthDataSizeForType(
			mysql_proto.LogEventType_XID_EVENT),
		Equals,
		1)
}

func (s *LogFileV4EventReaderSuite) Test55Stream(c *C) {
	s.WriteLogFileMagic()
	s.Write55FDE()
	s.WriteXidEvent()
	s.WriteRotateEvent()

	event, err := s.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*FormatDescriptionEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{})

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*XidEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{})

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*RotateEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{})

	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT_V1), NotNil)
	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT), IsNil)
}

func (s *LogFileV4EventReaderSuite) Test56StreamWithoutChecksum(c *C) {
	s.WriteLogFileMagic()
	s.Write56FDE()
	s.WriteXidEvent()
	s.WriteRotateEvent()

	event, err := s.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*FormatDescriptionEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{0, 0, 0, 0})

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*XidEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{})

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*RotateEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{})

	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT_V1), NotNil)
	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT), NotNil)
}

func (s *LogFileV4EventReaderSuite) Test56StreamWithChecksum(c *C) {
	s.checksumed = true

	s.WriteLogFileMagic()
	s.Write56FDE()
	s.WriteXidEvent()
	s.WriteRotateEvent()

	event, err := s.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*FormatDescriptionEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{0, 0, 0, 0})

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*XidEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte("asdf"))

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*RotateEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte("asdf"))

	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT_V1), NotNil)
	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT), NotNil)
}

func (s *LogFileV4EventReaderSuite) Test56RelayStreamFrom55Master(c *C) {
	s.WriteLogFileMagic()
	s.Write55Master56FDE()
	s.WriteXidEvent()
	s.WriteRotateEvent()

	event, err := s.NextEvent()
	c.Assert(err, IsNil)
	_, ok := event.(*FormatDescriptionEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{0, 0, 0, 0})

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*XidEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte(""))

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*RotateEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte(""))

	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT_V1), NotNil)
	c.Check(s.parsers.Get(mysql_proto.LogEventType_WRITE_ROWS_EVENT), IsNil)
}

func (s *LogFileV4EventReaderSuite) TestDisableChecksumMidStream(c *C) {
	s.checksumed = true

	s.WriteLogFileMagic()
	s.Write56FDE()
	s.WriteXidEvent()
	s.WriteRotateEvent()

	s.checksumed = false

	s.Write56FDE()
	s.WriteXidEvent()

	event, err := s.NextEvent()
	c.Assert(err, IsNil)
	fde, ok := event.(*FormatDescriptionEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{0, 0, 0, 0})
	c.Check(
		fde.ChecksumAlgorithm(),
		Equals,
		mysql_proto.ChecksumAlgorithm_CRC32)

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*XidEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte("asdf"))

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*RotateEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte("asdf"))

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	fde, ok = event.(*FormatDescriptionEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{0, 0, 0, 0})
	c.Check(fde.ChecksumAlgorithm(), Equals, mysql_proto.ChecksumAlgorithm_OFF)

	event, err = s.NextEvent()
	c.Assert(err, IsNil)
	_, ok = event.(*XidEvent)
	c.Check(ok, IsTrue)
	c.Check(event.Checksum(), DeepEquals, []byte{})
}

func (s *LogFileV4EventReaderSuite) TestClose(c *C) {
	s.checksumed = true

	s.WriteLogFileMagic()
	s.Write56FDE()
	s.WriteXidEvent()
	s.WriteRotateEvent()

	err := s.reader.Close()
	c.Assert(err, IsNil)

	event, err := s.NextEvent()
	c.Assert(event, IsNil)
	c.Assert(err, NotNil)
}
