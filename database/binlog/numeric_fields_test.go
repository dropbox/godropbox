package binlog

import (
	"bytes"
	"encoding/binary"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type NumericFieldsSuite struct {
}

var _ = Suite(&NumericFieldsSuite{})

func (s *NumericFieldsSuite) TestTinyBasic(c *C) {
	t := NewTinyFieldDescriptor(true)
	c.Check(t.IsNullable(), IsTrue)
	c.Check(t.Type(), Equals, mysql_proto.FieldType_TINY)
}

func (s *NumericFieldsSuite) TestTinyParseValue(c *C) {
	t := NewTinyFieldDescriptor(true)

	val, remaining, err := t.ParseValue(
		[]byte{123, 'r', 'e', 's', 't'})

	c.Assert(err, IsNil)
	real, ok := val.(uint64)
	c.Assert(ok, IsTrue)
	c.Check(real, Equals, uint64(123))
	c.Check(string(remaining), Equals, "rest")
}

func (s *NumericFieldsSuite) TestTinyParseValueTooFewBytes(c *C) {
	t := NewTinyFieldDescriptor(true)

	_, _, err := t.ParseValue([]byte{})

	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestShortBasic(c *C) {
	t := NewShortFieldDescriptor(false)
	c.Check(t.IsNullable(), IsFalse)
	c.Check(t.Type(), Equals, mysql_proto.FieldType_SHORT)
}

func (s *NumericFieldsSuite) TestShortParseValue(c *C) {
	t := NewShortFieldDescriptor(true)

	val, remaining, err := t.ParseValue(
		[]byte{0xed, 0xfe, 'r', 'e', 's', 't'})

	c.Assert(err, IsNil)
	real, ok := val.(uint64)
	c.Assert(ok, IsTrue)
	c.Check(real, Equals, uint64(0xfeed))
	c.Check(string(remaining), Equals, "rest")
}

func (s *NumericFieldsSuite) TestShortParseValueTooFewBytes(c *C) {
	t := NewShortFieldDescriptor(true)

	_, _, err := t.ParseValue([]byte{1})

	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestInt24ParseValue(c *C) {
	t := NewInt24FieldDescriptor(true)
	c.Check(t.Type(), Equals, mysql_proto.FieldType_INT24)

	val, remaining, err := t.ParseValue(
		[]byte{3, 2, 1, 'r', 'e', 's', 't'})

	c.Assert(err, IsNil)
	real, ok := val.(uint64)
	c.Assert(ok, IsTrue)
	c.Check(real, Equals, uint64(0x010203))
	c.Check(string(remaining), Equals, "rest")
}

func (s *NumericFieldsSuite) TestInt24ParseValueTooFewBytes(c *C) {
	t := NewInt24FieldDescriptor(true)

	_, _, err := t.ParseValue([]byte{1, 2})

	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestLongParseValue(c *C) {
	t := NewLongFieldDescriptor(true)
	c.Check(t.Type(), Equals, mysql_proto.FieldType_LONG)

	val, remaining, err := t.ParseValue(
		[]byte{4, 3, 2, 1, 'r', 'e', 's', 't'})

	c.Assert(err, IsNil)
	real, ok := val.(uint64)
	c.Assert(ok, IsTrue)
	c.Check(real, Equals, uint64(0x01020304))
	c.Check(string(remaining), Equals, "rest")
}

func (s *NumericFieldsSuite) TestLongParseValueTooFewBytes(c *C) {
	t := NewLongFieldDescriptor(true)

	_, _, err := t.ParseValue([]byte{1, 2, 3})

	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestLongLongParseValue(c *C) {
	t := NewLongLongFieldDescriptor(true)
	c.Check(t.Type(), Equals, mysql_proto.FieldType_LONGLONG)

	val, remaining, err := t.ParseValue(
		[]byte{8, 7, 6, 5, 4, 3, 2, 1, 'r', 'e', 's', 't'})

	c.Assert(err, IsNil)
	real, ok := val.(uint64)
	c.Assert(ok, IsTrue)
	c.Check(real, Equals, uint64(0x0102030405060708))
	c.Check(string(remaining), Equals, "rest")
}

func (s *NumericFieldsSuite) TestLongLongParseValueTooFewBytes(c *C) {
	t := NewLongLongFieldDescriptor(true)

	_, _, err := t.ParseValue([]byte{1, 2, 3, 4, 5, 6, 7})

	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestFloatInvalidMetadata(c *C) {
	_, _, err := NewFloatFieldDescriptor(true, []byte{8})
	c.Log(err)
	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestFloatMetadataTooShort(c *C) {
	_, _, err := NewFloatFieldDescriptor(true, []byte{})
	c.Log(err)
	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestFloatParseValue(c *C) {
	t, meta, err := NewFloatFieldDescriptor(true, []byte{4, 'f', 'o', 'o'})
	c.Assert(string(meta), Equals, "foo")
	c.Assert(err, IsNil)
	c.Check(t.Type(), Equals, mysql_proto.FieldType_FLOAT)

	expected := float32(3.1415)

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, expected)
	buf.WriteString("rest")

	input := buf.Bytes()
	c.Assert(len(input), Equals, 8)

	val, remaining, err := t.ParseValue(input)

	c.Assert(err, IsNil)
	real, ok := val.(float64)
	c.Assert(ok, IsTrue)
	c.Check(real, Equals, float64(expected))
	c.Check(string(remaining), Equals, "rest")
}

func (s *NumericFieldsSuite) TestFloatParseValueTooFewBytes(c *C) {
	t, _, err := NewFloatFieldDescriptor(true, []byte{4})
	c.Assert(err, IsNil)

	_, _, err = t.ParseValue([]byte{1, 2, 3})

	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestDoubleInvalidMetadata(c *C) {
	_, _, err := NewDoubleFieldDescriptor(true, []byte{7})
	c.Log(err)
	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestDoubleMetadataTooShort(c *C) {
	_, _, err := NewDoubleFieldDescriptor(true, []byte{})
	c.Log(err)
	c.Assert(err, Not(IsNil))
}

func (s *NumericFieldsSuite) TestDoubleParseValue(c *C) {
	t, meta, err := NewDoubleFieldDescriptor(true, []byte{8, 'f', 'o', 'o'})
	c.Assert(string(meta), Equals, "foo")
	c.Assert(err, IsNil)
	c.Check(t.Type(), Equals, mysql_proto.FieldType_DOUBLE)

	expected := float64(3.1415)

	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, expected)
	buf.WriteString("rest")

	input := buf.Bytes()
	c.Assert(len(input), Equals, 12)

	val, remaining, err := t.ParseValue(input)

	c.Assert(err, IsNil)
	real, ok := val.(float64)
	c.Assert(ok, IsTrue)
	c.Check(real, Equals, expected)
	c.Check(string(remaining), Equals, "rest")
}

func (s *NumericFieldsSuite) TestDoubleParseValueTooFewBytes(c *C) {
	t, _, err := NewDoubleFieldDescriptor(true, []byte{8})
	c.Assert(err, IsNil)

	_, _, err = t.ParseValue([]byte{1, 2, 3, 4, 5, 6, 7})

	c.Assert(err, Not(IsNil))
}

// TODO(patrick): implement decimal / new decimal field descriptors / tests.
