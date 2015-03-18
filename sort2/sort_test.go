package sort2

import (
	"testing"

	. "gopkg.in/check.v1"
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

func (s *SortSuite) TestSortByteArrays(c *C) {
	convert := func(s ...string) [][]byte {
		r := [][]byte{}
		for _, v := range s {
			r = append(r, []byte(v))
		}
		return r
	}
	slice := convert("5", "7", "3", "1", "9")
	ByteArrays(slice)

	c.Assert(slice, DeepEquals, convert("1", "3", "5", "7", "9"))
}
