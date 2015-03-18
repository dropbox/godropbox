package binlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/errors"
	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

const testDir = "/dir"
const testRelayPrefix = "relay."
const testBinPrefix = "bin."

type LogStreamV4EventReaderSuite struct {
	files map[string]*MockLogFile
}

var _ = Suite(&LogStreamV4EventReaderSuite{})

func (s *LogStreamV4EventReaderSuite) SetUpTest(c *C) {
	s.files = make(map[string]*MockLogFile)
}

func (s *LogStreamV4EventReaderSuite) GetFile(
	prefix string, num int) *MockLogFile {

	path := logName(prefix, num)
	f, ok := s.files[path]
	if ok {
		return f
	}

	f = NewMockLogFile()
	f.WriteLogFileMagic()
	f.WriteFDE()
	s.files[path] = f
	return f
}

func (s *LogStreamV4EventReaderSuite) NewStream(
	prefix string,
	num int) EventReader {

	isRelay := false
	if prefix == testRelayPrefix {
		isRelay = true
	}
	return NewLogStreamV4EventReaderWithLogFileReaderCreator(
		testDir,
		prefix,
		uint(num),
		isRelay,
		Logger{
			Fatalf:       log.Fatalf,
			Infof:        log.Printf,
			VerboseInfof: log.Printf,
		},
		s.newFakeLogReader)
}

func (s *LogStreamV4EventReaderSuite) newFakeLogReader(
	dir string,
	filename string,
	parsers V4EventParserMap) (EventReader, error) {

	buf, ok := s.files[filename]
	if !ok {
		return nil, errors.Newf("Missing: %s", filename)
	}

	delete(s.files, filename)

	return NewLogFileV4EventReader(
		buf.GetReader(),
		filename,
		parsers,
		Logger{
			Fatalf:       log.Fatalf,
			Infof:        log.Printf,
			VerboseInfof: log.Printf,
		}), nil
}

func (s *LogStreamV4EventReaderSuite) TestRotateBasic(c *C) {
	prefix := testBinPrefix

	f0 := s.GetFile(prefix, 0)
	f0.WriteXid(0)
	f0.WriteRotate(prefix, 1)

	f1 := s.GetFile(prefix, 1)
	f1.WriteXid(1)
	f1.WriteRotate(prefix, 2)

	f2 := s.GetFile(prefix, 2)
	f2.WriteXid(2)
	f2.WriteRotate(prefix, 3)

	stream := s.NewStream(prefix, 0)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 0
	e := Next()
	_, ok := e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(0))

	e = Next()
	r, ok := e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(prefix, 1))

	// log file 1
	e = Next()
	_, ok = e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(1))

	e = Next()
	r, ok = e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(prefix, 2))

	// log file 2
	e = Next()
	_, ok = e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(2))

	e = Next()
	r, ok = e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(prefix, 3))

	// simulate rotate event written, but new file not opened yet.
	e, err := stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	_, ok = err.(*FailedToOpenFileError)
	c.Check(ok, IsTrue)

	f3 := s.GetFile(prefix, 3)
	f3.WriteXid(3)

	// log file 3
	e = Next()
	_, ok = e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(3))

	e, err = stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	c.Check(err, Equals, io.EOF)
}

func (s *LogStreamV4EventReaderSuite) TestStopBasic(c *C) {
	prefix := testBinPrefix

	f0 := s.GetFile(prefix, 0)
	f0.WriteXid(0)
	f0.WriteStop()

	f1 := s.GetFile(prefix, 1)
	f1.WriteXid(1)
	f1.WriteStop()

	stream := s.NewStream(prefix, 0)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 0
	e := Next()
	_, ok := e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(0))

	e = Next()
	_, ok = e.(*StopEvent)
	c.Assert(ok, IsTrue)

	// log file 1
	e = Next()
	_, ok = e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(1))

	e = Next()
	_, ok = e.(*StopEvent)
	c.Assert(ok, IsTrue)

	// since next file is missing, opening next file should return error.
	e, err := stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	ferr, ok := err.(*FailedToOpenFileError)
	c.Assert(ok, IsTrue)
	c.Assert(ferr.LogFileNum, Equals, uint(2))
}

func (s *LogStreamV4EventReaderSuite) TestRotateRollOver(c *C) {
	prefix := testBinPrefix

	f0 := s.GetFile(prefix, maxLogFileNum)
	f0.WriteXid(maxLogFileNum)
	f0.WriteRotate(prefix, 0)

	f1 := s.GetFile(prefix, 0)
	f1.WriteXid(0)

	stream := s.NewStream(prefix, maxLogFileNum)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 999999
	e := Next()
	_, ok := e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(maxLogFileNum))

	e = Next()
	r, ok := e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(prefix, 0))

	// log file 0
	e = Next()
	_, ok = e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(0))

	e, err := stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	c.Check(err, Equals, io.EOF)
}

func (s *LogStreamV4EventReaderSuite) TestRotateJumped1(c *C) {
	prefix := testBinPrefix

	f0 := s.GetFile(prefix, maxLogFileNum)
	f0.WriteXid(maxLogFileNum)
	f0.WriteRotate(prefix, 1)

	f1 := s.GetFile(prefix, 1)
	f1.WriteXid(1)

	stream := s.NewStream(prefix, maxLogFileNum)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 999999
	e := Next()
	_, ok := e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(maxLogFileNum))

	e, err := stream.NextEvent()
	c.Assert(e, NotNil)
	c.Assert(err, NotNil)

	// should have rotate to 0
	r, ok := e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(prefix, 1))

	_, ok = err.(*InvalidRotationError)
	c.Assert(ok, IsTrue)

	e, err = stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err, Equals, io.EOF)
}

func (s *LogStreamV4EventReaderSuite) TestRotateJumped2(c *C) {
	prefix := testBinPrefix

	f0 := s.GetFile(prefix, 123)
	f0.WriteXid(123)
	f0.WriteRotate(prefix, 125)

	f1 := s.GetFile(prefix, 125)
	f1.WriteXid(125)

	stream := s.NewStream(prefix, 123)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 123
	e := Next()
	_, ok := e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(123))

	e, err := stream.NextEvent()
	c.Assert(e, NotNil)
	c.Assert(err, NotNil)

	// should have rotate to 124
	r, ok := e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(prefix, 125))

	_, ok = err.(*InvalidRotationError)
	c.Assert(ok, IsTrue)

	e, err = stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err, Equals, io.EOF)
}

func (s *LogStreamV4EventReaderSuite) TestInvalidLogPrefix(c *C) {
	prefix := testBinPrefix
	badPrefix := "bad."

	f0 := s.GetFile(prefix, 0)
	f0.WriteXid(0)
	f0.WriteRotate(badPrefix, 1)
	f0.WriteXid(1)

	f1 := s.GetFile(badPrefix, 1)
	f1.WriteXid(123)

	stream := s.NewStream(prefix, 0)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 1
	e := Next()
	_, ok := e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(0))

	// bad rotation event
	e, err := stream.NextEvent()
	c.Assert(e, NotNil)
	c.Assert(err, NotNil)

	r, ok := e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(badPrefix, 1))

	_, ok = err.(*InvalidRotationError)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(1))

	e, err = stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, Equals, io.EOF)
}

func (s *LogStreamV4EventReaderSuite) TestInvalidLogNumber(c *C) {
	prefix := testBinPrefix

	f0 := s.GetFile(prefix, 0)
	f0.WriteXid(0)

	// rotate event bin.xxxxxx
	data := &bytes.Buffer{}
	binary.Write(data, binary.LittleEndian, uint64(4))
	data.WriteString(prefix)
	data.WriteString("xxxxxx")
	bytes, _ := CreateEventBytes(
		uint32(0),
		uint8(mysql_proto.LogEventType_ROTATE_EVENT),
		uint32(1),
		uint32(1234),
		uint16(1),
		data.Bytes())
	f0.Write(bytes)

	f0.WriteXid(1)

	stream := s.NewStream(prefix, 0)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 1
	e := Next()
	_, ok := e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(0))

	// bad rotation event
	e, err := stream.NextEvent()
	c.Assert(e, NotNil)
	c.Assert(err, NotNil)

	r, ok := e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, testBinPrefix+"xxxxxx")

	_, ok = err.(*InvalidRotationError)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(1))

	e, err = stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, Equals, io.EOF)
}

func (s *LogStreamV4EventReaderSuite) TestRelayLog(c *C) {
	prefix := testRelayPrefix

	f0 := s.GetFile(prefix, 1)
	f0.WriteXid(1)
	// relay log has master's bin log rotate event midstream
	f0.WriteRotate(testBinPrefix, 12345)
	f0.WriteXid(10)
	f0.WriteRotate(prefix, 2)

	f1 := s.GetFile(prefix, 2)
	f1.WriteXid(2)
	// relay log has master's bin log fde midstream
	f1.WriteFDE()
	f1.WriteXid(20)
	// relay log has master's bin log rotate event midstream
	f1.WriteRotate(testBinPrefix, 333)
	f1.WriteXid(200)

	stream := s.NewStream(prefix, 1)

	Next := func() Event {
		e, err := stream.NextEvent()
		c.Assert(err, IsNil)
		c.Assert(e, NotNil)
		return e
	}

	// log file 1
	e := Next()
	x, ok := e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(1))

	e = Next()
	r, ok := e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(testBinPrefix, 12345))

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(10))

	// log file 2
	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(2))

	e = Next()
	_, ok = e.(*FormatDescriptionEvent)
	c.Assert(ok, IsTrue)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(20))

	e = Next()
	r, ok = e.(*RotateEvent)
	c.Assert(ok, IsTrue)
	c.Check(string(r.NewLogName()), Equals, logName(testBinPrefix, 333))

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(200))

	e, err := stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	c.Check(err, Equals, io.EOF)

	// write one more event to log file 1
	f1.WriteRotate(prefix, 3)

	e, err = stream.NextEvent()
	c.Assert(e, IsNil)
	c.Assert(err, NotNil)
	_, ok = err.(*FailedToOpenFileError)
	c.Check(ok, IsTrue)

	// write to 3rd relay log file.
	f2 := s.GetFile(prefix, 3)
	f2.WriteXid(300)

	e = Next()
	x, ok = e.(*XidEvent)
	c.Assert(ok, IsTrue)
	c.Check(x.Xid(), Equals, uint64(300))

}
