package binlog

import (
	"time"

	. "gopkg.in/check.v1"

	. "godropbox/gocheck2"
)

type TemporalFieldsSuite struct {
}

var _ = Suite(&TemporalFieldsSuite{})

func (s *TemporalFieldsSuite) TestDateField(c *C) {
	d := NewDateFieldDescriptor(Nullable)
	c.Assert(d.IsNullable(), IsTrue)
	// Only consumes the first three bytes. Generated using:
	//   python -c "print format(2019*32*16 + 1*32 + 17, '06x')"
	// and then put in little-endian order.
	// https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
	val, remaining, err := d.ParseValue([]byte{0x31, 0xc6, 0x0f, 0x96, 0xfd, 0xd8})
	c.Assert(err, NoErr)
	c.Assert(val, FitsTypeOf, time.Time{})
	c.Assert(val.(time.Time).Equal(time.Date(2019, 01, 17, 0, 0, 0, 0, time.UTC)), IsTrue)
	c.Assert(remaining, DeepEqualsPretty, []byte{0x96, 0xfd, 0xd8})
}
