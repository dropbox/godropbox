package binlog

import (
	"godropbox/errors"
	"testing"

	"vitess.io/vitess/go/mysql"

	mysql_proto "dropbox/proto/mysql"

	"github.com/stretchr/testify/suite"
)

type BinlogStreamV4EventReaderSuite struct {
	suite.Suite
	packets  [][]byte
	fakeData []byte
}

func TestBinlogStreamV4EventReaderSuite(t *testing.T) {
	suite.Run(t, new(BinlogStreamV4EventReaderSuite))
}

func (s *BinlogStreamV4EventReaderSuite) SetupTest() {
	numEvents := 10
	s.packets = make([][]byte, numEvents)
	s.fakeData = []byte("fake")
	for i := range s.packets {
		eventBytes, err := CreateEventBytes(
			uint32(i), // timestamp
			uint8(mysql_proto.LogEventType_Type(0x12)),
			uint32(i), // server id
			uint32(i), // next position
			uint16(i), // flags,
			s.fakeData,
		)
		s.Require().NoError(err)
		packet := append([]byte{mysql.OKPacket}, eventBytes...)
		s.packets[i] = packet
	}
}

type mockMysqlConn struct {
	offset  int
	packets [][]byte
}

func (m *mockMysqlConn) ReadPacket() ([]byte, error) {
	if m.offset >= len(m.packets) {
		return nil, errors.Newf("ran out of packets")
	}
	packet := m.packets[m.offset]
	m.offset++
	return packet, nil
}

func (s *BinlogStreamV4EventReaderSuite) TestParseSimpleEvents() {

	mockConn := &mockMysqlConn{packets: s.packets}

	reader := NewRawV4StreamReader(mockConn)

	for i := range s.packets {
		event, err := reader.NextEvent()
		s.Require().NoError(err)

		s.Require().Equal(uint32(i), event.Timestamp())
		s.Require().Equal(mysql_proto.LogEventType_Type(0x12), event.EventType())
		s.Require().Equal(uint32(i), event.ServerId())
		s.Require().Equal(uint32(i), event.NextPosition())
		s.Require().Equal(uint16(i), event.Flags())
		s.Require().Equal([]byte{}, event.ExtraHeaders())
		s.Require().Equal([]byte{}, event.FixedLengthData())
		s.Require().Equal(s.fakeData, event.VariableLengthData())
	}
	_, err := reader.NextEvent()
	s.Require().NotNil(err)
}

func (s *BinlogStreamV4EventReaderSuite) TestParsingErrorInStream() {
	errIndex := 7
	s.packets[errIndex][0] = mysql.ErrPacket
	mockConn := &mockMysqlConn{packets: s.packets}
	reader := NewRawV4StreamReader(mockConn)

	for i := 0; i < errIndex; i++ {
		event, err := reader.NextEvent()
		s.Require().NoError(err)

		s.Require().Equal(uint32(i), event.Timestamp())
		s.Require().Equal(mysql_proto.LogEventType_Type(0x12), event.EventType())
		s.Require().Equal(uint32(i), event.ServerId())
		s.Require().Equal(uint32(i), event.NextPosition())
		s.Require().Equal(uint16(i), event.Flags())
		s.Require().Equal([]byte{}, event.ExtraHeaders())
		s.Require().Equal([]byte{}, event.FixedLengthData())
		s.Require().Equal(s.fakeData, event.VariableLengthData())
	}
	_, err := reader.NextEvent()
	s.Require().NotNil(err)
}
