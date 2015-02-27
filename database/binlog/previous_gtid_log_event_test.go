package binlog

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
	. "gopkg.in/check.v1"
)

type PreviousGtidsLogEventSuite struct {
	EventParserSuite
}

var _ = Suite(&PreviousGtidsLogEventSuite{})

func (s *PreviousGtidsLogEventSuite) serialize(set GtidSet) []byte {
	data := &bytes.Buffer{}

	// n_sids
	binary.Write(data, LittleEndian, uint64(len(set)))
	for sid, intervals := range set {
		// sid + n_intervals
		data.WriteString(sid)
		binary.Write(data, LittleEndian, uint64(len(intervals)))
		for _, interval := range intervals {
			// start + end
			binary.Write(data, LittleEndian, interval.Start)
			binary.Write(data, LittleEndian, interval.End)
		}
	}

	return data.Bytes()
}

var testCases = []GtidSet{
	GtidSet{},
	GtidSet{
		strings.Repeat("a", 16): []GtidRange{
			GtidRange{0, 1},
		},
	},
	GtidSet{
		strings.Repeat("a", 16): []GtidRange{
			GtidRange{5, 10},
			GtidRange{10, 20},
		},
		strings.Repeat("b", 16): []GtidRange{
			GtidRange{5, 10},
			GtidRange{10, 20},
		},
	},
}

func (s *PreviousGtidsLogEventSuite) TestSuccess(c *C) {
	for _, test := range testCases {
		s.WriteEvent(mysql_proto.LogEventType_PREVIOUS_GTIDS_LOG_EVENT, 0, s.serialize(test))
		event, err := s.NextEvent()
		c.Assert(err, IsNil)

		pgle, ok := event.(*PreviousGtidsLogEvent)
		c.Assert(ok, IsTrue)
		for sid, intervals := range pgle.GtidSet() {
			println(sid)
			for _, interval := range intervals {
				println(interval.Start, interval.End)
			}
		}
		c.Assert(reflect.DeepEqual(test, pgle.GtidSet()), IsTrue)
	}
}

// Not enough bytes to read n_sids
func (s *PreviousGtidsLogEventSuite) TestFailure(c *C) {
	data := s.serialize(testCases[2])

	testCases := [][]byte{
		// Not enough bytes to read n_sids
		data[:6],

		// sid missing
		data[:8],

		// n_intervals missing
		data[:24],

		// n_intervals missing end
		data[:32],

		// fewer n_intervals available then specified
		data[:40],

		// extra bytes at the end
		append(data, []byte("extra bytes")...),
	}

	for _, test := range testCases {
		s.SetUpTest(c)

		s.WriteEvent(mysql_proto.LogEventType_PREVIOUS_GTIDS_LOG_EVENT, 0, test)
		_, err := s.NextEvent()
		c.Assert(err, NotNil)
	}
}
