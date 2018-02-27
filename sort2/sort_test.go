package sort2

import (
	"testing"

	. "gopkg.in/check.v1"
	"time"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SortSuite struct {
}

var _ = Suite(&SortSuite{})

func (s *SortSuite) TestSortUint(c *C) {
	slice := UintSlice([]uint{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]uint(slice), DeepEquals, []uint{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortInt8(c *C) {
	slice := Int8Slice([]int8{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]int8(slice), DeepEquals, []int8{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint8(c *C) {
	slice := Uint8Slice([]uint8{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]uint8(slice), DeepEquals, []uint8{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint16(c *C) {
	slice := Uint16Slice([]uint16{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]uint16(slice), DeepEquals, []uint16{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortInt16(c *C) {
	slice := Int16Slice([]int16{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]int16(slice), DeepEquals, []int16{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortInt32(c *C) {
	slice := Int32Slice([]int32{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]int32(slice), DeepEquals, []int32{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortInt64(c *C) {
	slice := Int64Slice([]int64{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]int64(slice), DeepEquals, []int64{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint32(c *C) {
	slice := Uint32Slice([]uint32{19, 21, 30, 32, 6, 12, 5, 4, 8, 1})
	slice.Sort()
	c.Assert([]uint32(slice), DeepEquals, []uint32{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint64(c *C) {
	slice := Uint64Slice([]uint64{9, 2, 3, 7, 6, 1, 5, 4, 8, 0})
	slice.Sort()

	c.Assert([]uint64(slice), DeepEquals, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
}

func (s *SortSuite) TestSortFloat32(c *C) {
	slice := Float32Slice([]float32{9.0, 2.0, 3.0, 7.0, 6.0, 1.0, 5.0, 4.0, 8.0, 0.0})
	slice.Sort()

	c.Assert([]float32(slice), DeepEquals, []float32{0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0})
}

func (s *SortSuite) TestSortFloat64(c *C) {
	slice := Float64Slice([]float64{9.0, 2.0, 3.0, 7.0, 6.0, 1.0, 5.0, 4.0, 8.0, 0.0})
	slice.Sort()

	c.Assert([]float64(slice), DeepEquals, []float64{0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0})
}

func (s *SortSuite) TestSortByteArraySlice(c *C) {
	slice := ByteArraySlice([][]byte{
		[]byte("E"),
		[]byte("Z"),
		[]byte("A"),
		[]byte("C"),
		[]byte("N"),
		[]byte("P"),
	})

	slice.Sort()

	c.Assert([][]byte(slice), DeepEquals, [][]byte{
		[]byte("A"),
		[]byte("C"),
		[]byte("E"),
		[]byte("N"),
		[]byte("P"),
		[]byte("Z"),
	})
}

func (s *SortSuite) TestSortTimeSlice(c *C) {
	slice := TimeSlice([]time.Time{
		time.Date(2016, time.August, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.September, 17, 0, 0, 0, 0, time.UTC),
		time.Date(2015, time.December, 10, 0, 0, 0, 0, time.UTC),
	})
	slice.Sort()

	sortedTime := []time.Time{
		time.Date(2015, time.December, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.August, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.September, 17, 0, 0, 0, 0, time.UTC),
	}

	c.Assert([]time.Time(slice), DeepEquals, sortedTime)
}

func (s *SortSuite) TestSortByteArrays(c *C) {
	convert := func(s ...string) [][]byte {
		r := make([][]byte, 0)
		for _, v := range s {
			r = append(r, []byte(v))
		}
		return r
	}
	slice := convert("5", "7", "3", "1", "9")
	ByteArrays(slice)

	c.Assert(slice, DeepEquals, convert("1", "3", "5", "7", "9"))
}

func (s *SortSuite) TestSortUints(c *C) {
	slice := []uint{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Uints(slice)
	c.Assert([]uint(slice), DeepEquals, []uint{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint8s(c *C) {
	slice := []uint8{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Uint8s(slice)
	c.Assert([]uint8(slice), DeepEquals, []uint8{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint16s(c *C) {
	slice := []uint16{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Uint16s(slice)
	c.Assert([]uint16(slice), DeepEquals, []uint16{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint32s(c *C) {
	slice := []uint32{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Uint32s(slice)
	c.Assert([]uint32(slice), DeepEquals, []uint32{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortUint64s(c *C) {
	slice := []uint64{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Uint64s(slice)
	c.Assert([]uint64(slice), DeepEquals, []uint64{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortInt8s(c *C) {
	slice := []int8{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Int8s(slice)
	c.Assert([]int8(slice), DeepEquals, []int8{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortInt16s(c *C) {
	slice := []int16{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Int16s(slice)
	c.Assert([]int16(slice), DeepEquals, []int16{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestSortInt32s(c *C) {
	slice := []int32{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Int32s(slice)
	c.Assert([]int32(slice), DeepEquals, []int32{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestFloat32s(c *C) {
	slice := []float32{19.0, 21.0, 30.0, 32.0, 6.0, 12.0, 5.0, 4.0, 8.0, 1.0}
	Float32s(slice)
	c.Assert([]float32(slice), DeepEquals, []float32{1.0, 4.0, 5.0, 6.0, 8.0, 12.0, 19.0, 21.0, 30.0, 32.0})
}

func (s *SortSuite) TestFloat64s(c *C) {
	slice := []float64{19.0, 21.0, 30.0, 32.0, 6.0, 12.0, 5.0, 4.0, 8.0, 1.0}
	Float64s(slice)
	c.Assert([]float64(slice), DeepEquals, []float64{1.0, 4.0, 5.0, 6.0, 8.0, 12.0, 19.0, 21.0, 30.0, 32.0})
}

func (s *SortSuite) TestSortInt64s(c *C) {
	slice := []int64{19, 21, 30, 32, 6, 12, 5, 4, 8, 1}
	Int64s(slice)
	c.Assert([]int64(slice), DeepEquals, []int64{1, 4, 5, 6, 8, 12, 19, 21, 30, 32})
}

func (s *SortSuite) TestTimes(c *C) {
	slice := TimeSlice([]time.Time{
		time.Date(2016, time.August, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.September, 17, 0, 0, 0, 0, time.UTC),
		time.Date(2015, time.December, 10, 0, 0, 0, 0, time.UTC),
	})
	Times(slice)
	sortedTime := []time.Time{
		time.Date(2015, time.December, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.August, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.September, 17, 0, 0, 0, 0, time.UTC),
	}

	c.Assert([]time.Time(slice), DeepEquals, sortedTime)
}
