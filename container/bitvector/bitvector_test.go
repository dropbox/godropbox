package bitvector

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type BitVectorSuite struct {
	vector *BitVector
}

var _ = Suite(&BitVectorSuite{})

func (s *BitVectorSuite) SetUpTest(c *C) {
	// This sets elements 4-12
	data := []byte{0xF0, 0x0F}
	length := 12
	s.vector = NewBitVector(data, length)
}

func (s *BitVectorSuite) TestElement(c *C) {
	for i := 0; i < 4; i++ {
		c.Assert(s.vector.Element(i), Equals, byte(0))
	}
	for i := 4; i < 12; i++ {
		c.Assert(s.vector.Element(i), Equals, byte(1))
	}
}

func (s *BitVectorSuite) TestInsert(c *C) {
	for i := 0; i < 4; i++ {
		s.vector.Insert(0, 8)
	}
	c.Assert(s.vector.Bytes(), DeepEquals, []byte{0xF0, 0xF0})
}

func (s *BitVectorSuite) TestAppend(c *C) {
	for i := 0; i < 4; i++ {
		if i%2 == 0 {
			s.vector.Append(0)
		} else {
			s.vector.Append(1)
		}
	}
	c.Assert(s.vector.Bytes(), DeepEquals, []byte{0xF0, 0xAF})
}

func (s *BitVectorSuite) TestSet(c *C) {
	for i := 4; i < 8; i++ {
		if i%2 == 0 {
			s.vector.Set(0, i)
		}
	}
	c.Assert(s.vector.Bytes(), DeepEquals, []byte{0xA0, 0x0F})
}

func (s *BitVectorSuite) TestDelete(c *C) {
	for i := 0; i < 4; i++ {
		s.vector.Delete(8)
	}
	c.Assert(s.vector.Bytes(), DeepEquals, []byte{0xF0})
}
