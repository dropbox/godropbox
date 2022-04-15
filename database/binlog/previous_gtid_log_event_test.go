package binlog

import (
	"reflect"
	"strings"

	mysql_proto "dropbox/proto/mysql"
	. "godropbox/gocheck2"

	. "gopkg.in/check.v1"
)

type PreviousGtidsLogEventSuite struct {
	EventParserSuite
}

var _ = Suite(&PreviousGtidsLogEventSuite{})

var testCases = []GtidSet{
	{},
	{
		strings.Repeat("a", 16): []GtidRange{
			{1, 2},
		},
	},
	{
		strings.Repeat("a", 16): []GtidRange{
			{5, 10},
			{10, 20},
		},
		strings.Repeat("b", 16): []GtidRange{
			{5, 10},
			{10, 20},
		},
	},
	{
		strings.Repeat("a", 16): []GtidRange{
			{5, 10},
			{13, 14},
			{15, 19},
		},
	},
}

// NOTE: The end will get trimmed by 1 since in-memory GTIDSet
// representation would be [inclusive,exclusive]. But when it
// get printed out, it should be [inclusive, inclusive]
var testCasesStrs = []string{
	"",
	"61616161-6161-6161-6161-616161616161:1",
	"61616161-6161-6161-6161-616161616161:5-9:10-19," +
		"62626262-6262-6262-6262-626262626262:5-9:10-19",
	"61616161-6161-6161-6161-616161616161:5-9:13:15-18",
}

func (s *PreviousGtidsLogEventSuite) TestSuccess(c *C) {
	for i, test := range testCases {
		s.WriteEvent(mysql_proto.LogEventType_PREVIOUS_GTIDS_LOG_EVENT, 0, serializeGtidSet(test))
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
		c.Assert(pgle.GtidSet().String(), Equals, testCasesStrs[i])
	}
}

// Not enough bytes to read n_sids
func (s *PreviousGtidsLogEventSuite) TestFailure(c *C) {
	data := serializeGtidSet(testCases[2])

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
