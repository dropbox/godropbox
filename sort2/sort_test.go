package sort2

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SortSuite struct {
}

var _ = Suite(&SortSuite{})

func (s *SortSuite) TestSortUint64(c *C) {
	slice := Uint64Slice([]uint64{9, 2, 3, 7, 6, 1, 5, 4, 8, 0})
	slice.Sort()

	c.Assert([]uint64(slice), DeepEquals, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
}
