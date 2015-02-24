package binlog

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type StringFieldsSuite struct {
}

var _ = Suite(&StringFieldsSuite{})

func (s *StringFieldsSuite) TestNullParseValue(c *C) {
	d := NewNullFieldDescriptor(true)
	c.Check(d.IsNullable(), IsTrue)
	c.Check(d.Type(), Equals, mysql_proto.FieldType_NULL)

	val, remaining, err := d.ParseValue([]byte("foo"))
	c.Check(val, IsNil)
	c.Check(string(remaining), Equals, "foo")
	c.Check(err, IsNil)
}

func (s *StringFieldsSuite) TestStringParseValueOneByteLength(c *C) {
	d := NewStringFieldDescriptor(mysql_proto.FieldType_STRING, true, 5)
	c.Check(d.IsNullable(), IsTrue)
	c.Check(d.Type(), Equals, mysql_proto.FieldType_STRING)

	sd, ok := d.(*stringFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(sd.maxLength, Equals, 5)
	c.Check(sd.packedLength, Equals, 1)

	val, remaining, err := d.ParseValue(
		[]byte{3, 'f', 'o', 'o', 'r', 'e', 's', 't'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "rest")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo\x00\x00")
}

func (s *StringFieldsSuite) TestStringParseValueTwoByteLength(c *C) {
	d := NewStringFieldDescriptor(mysql_proto.FieldType_VAR_STRING, false, 256)
	c.Check(d.IsNullable(), IsFalse)
	c.Check(d.Type(), Equals, mysql_proto.FieldType_VAR_STRING)

	sd, ok := d.(*stringFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(sd.maxLength, Equals, 256)
	c.Check(sd.packedLength, Equals, 2)

	val, remaining, err := d.ParseValue(
		[]byte{3, 0, 'f', 'o', 'o', 'r', 'e', 's', 't'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "rest")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo")
}

func (s *StringFieldsSuite) TestStringTooFewLengthBytes(c *C) {
	d := NewStringFieldDescriptor(mysql_proto.FieldType_STRING, true, 1234)

	_, _, err := d.ParseValue([]byte{})
	c.Check(err, Not(IsNil))

	_, _, err = d.ParseValue([]byte{3})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestStringTooFewDataBytes(c *C) {
	d := NewStringFieldDescriptor(mysql_proto.FieldType_STRING, true, 1234)

	_, _, err := d.ParseValue([]byte{3, 0, 'a', 'b'})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestVarcharTooFewMetadataBytes(c *C) {
	_, _, err := NewVarcharFieldDescriptor(true, []byte{1})

	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestVarcharParseValueOneByteLength(c *C) {
	d, remaining, err := NewVarcharFieldDescriptor(
		true,
		[]byte{255, 0, 'a', 'b', 'c'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "abc")
	c.Check(d.IsNullable(), IsTrue)
	c.Check(d.Type(), Equals, mysql_proto.FieldType_VARCHAR)

	sd, ok := d.(*stringFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(sd.maxLength, Equals, 255)
	c.Check(sd.packedLength, Equals, 1)

	val, remaining, err := d.ParseValue(
		[]byte{3, 'f', 'o', 'o', 'r', 'e', 's', 't'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "rest")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo")
}

func (s *StringFieldsSuite) TestVarcharParseValueTwoBytesLength(c *C) {
	d, remaining, err := NewVarcharFieldDescriptor(
		false,
		[]byte{0, 1, 'a', 'b', 'c'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "abc")
	c.Check(d.IsNullable(), IsFalse)
	c.Check(d.Type(), Equals, mysql_proto.FieldType_VARCHAR)

	sd, ok := d.(*stringFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(sd.maxLength, Equals, 256)
	c.Check(sd.packedLength, Equals, 2)

	val, remaining, err := d.ParseValue(
		[]byte{3, 0, 'f', 'o', 'o', 'r', 'e', 's', 't'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "rest")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo")
}

func (s *StringFieldsSuite) TestVarcharTooFewLengthBytes(c *C) {
	d, _, err := NewVarcharFieldDescriptor(true, []byte{0, 1})
	c.Check(err, IsNil)

	_, _, err = d.ParseValue([]byte{3})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestVarcharTooFewDataBytes(c *C) {
	d, _, err := NewVarcharFieldDescriptor(true, []byte{0, 1})
	c.Check(err, IsNil)

	_, _, err = d.ParseValue([]byte{3, 0, 'a'})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestBlobTooFewMetadataBytes(c *C) {
	_, _, err := NewBlobFieldDescriptor(true, []byte{})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestBlobBadPackedLength(c *C) {
	_, _, err := NewBlobFieldDescriptor(true, []byte{5})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestBlobParseValueOneByteLength(c *C) {
	d, remaining, err := NewBlobFieldDescriptor(true, []byte{1, 'a', 'b', 'c'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "abc")
	c.Check(d.IsNullable(), IsTrue)
	c.Check(d.Type(), Equals, mysql_proto.FieldType_BLOB)

	bd, ok := d.(*blobFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(bd.packedLength, Equals, 1)

	val, remaining, err := d.ParseValue([]byte{3, 'f', 'o', 'o', 'b', 'a', 'r'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "bar")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo")
}

func (s *StringFieldsSuite) TestBlobParseValueTwoBytesLength(c *C) {
	d, remaining, err := NewBlobFieldDescriptor(false, []byte{2, 'a', 'b', 'c'})
	c.Check(err, IsNil)
	c.Check(d.IsNullable(), IsFalse)

	bd, ok := d.(*blobFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(bd.packedLength, Equals, 2)

	val, remaining, err := d.ParseValue(
		[]byte{3, 0, 'f', 'o', 'o', 'b', 'a', 'r'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "bar")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo")
}

func (s *StringFieldsSuite) TestBlobParseValueThreeBytesLength(c *C) {
	d, _, err := NewBlobFieldDescriptor(true, []byte{3, 'a', 'b', 'c'})
	c.Check(err, IsNil)

	bd, ok := d.(*blobFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(bd.packedLength, Equals, 3)

	val, remaining, err := d.ParseValue(
		[]byte{3, 0, 0, 'f', 'o', 'o', 'b', 'a', 'r'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "bar")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo")
}

func (s *StringFieldsSuite) TestBlobParseValueFourBytesLength(c *C) {
	d, _, err := NewBlobFieldDescriptor(true, []byte{4, 'a', 'b', 'c'})
	c.Check(err, IsNil)

	bd, ok := d.(*blobFieldDescriptor)
	c.Check(ok, IsTrue)
	c.Check(bd.packedLength, Equals, 4)

	val, remaining, err := d.ParseValue(
		[]byte{3, 0, 0, 0, 'f', 'o', 'o', 'b', 'a', 'r'})
	c.Check(err, IsNil)
	c.Check(string(remaining), Equals, "bar")
	real, ok := val.([]byte)
	c.Check(ok, IsTrue)
	c.Check(string(real), Equals, "foo")
}

func (s *StringFieldsSuite) TestBlobValueTooLong(c *C) {
	d, _, err := NewBlobFieldDescriptor(true, []byte{4, 'a', 'b', 'c'})
	c.Check(err, IsNil)

	_, _, err = d.ParseValue(
		[]byte{0xff, 0xff, 0xff, 0xff, 'f', 'o', 'o', 'b', 'a', 'r'})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestBlobTooFewLengthBytes(c *C) {
	d, _, err := NewBlobFieldDescriptor(true, []byte{4, 'a', 'b', 'c'})
	c.Check(err, IsNil)

	_, _, err = d.ParseValue([]byte{1, 2, 3})
	c.Check(err, Not(IsNil))
}

func (s *StringFieldsSuite) TestBlobTooFewDataBytes(c *C) {
	d, _, err := NewBlobFieldDescriptor(true, []byte{4, 'a', 'b', 'c'})
	c.Check(err, IsNil)

	_, _, err = d.ParseValue([]byte{3, 0, 0, 0, 'f', 'o'})
	c.Check(err, Not(IsNil))
}

type ParseTypeAndLengthSuite struct {
}

var _ = Suite(&ParseTypeAndLengthSuite{})

func (s *ParseTypeAndLengthSuite) TestTooFewBytes(c *C) {
	_, _, _, err := parseTypeAndLength([]byte{1})
	c.Check(err, Not(IsNil))
}

func (s *ParseTypeAndLengthSuite) TestMangledEncoding(c *C) {
	// set = 248, length = 1020
	//
	// >>> 248 ^ ((1020 & 0x300) >> 4)
	// 200
	// >>> 1020 & 0xff
	// 252
	fieldType, length, remaining, err := parseTypeAndLength(
		[]byte{200, 252, 'f', 'o', 'o'})

	c.Check(fieldType, Equals, mysql_proto.FieldType_SET)
	c.Check(length, Equals, 1020)
	c.Check(string(remaining), Equals, "foo")
	c.Check(err, IsNil)
}

func (s *ParseTypeAndLengthSuite) TestUnmangledEncoding(c *C) {
	fieldType, length, remaining, err := parseTypeAndLength(
		[]byte{byte(mysql_proto.FieldType_SET), 123, 'f', 'o', 'o'})

	c.Check(fieldType, Equals, mysql_proto.FieldType_SET)
	c.Check(length, Equals, 123)
	c.Check(string(remaining), Equals, "foo")
	c.Check(err, IsNil)
}

func (s *ParseTypeAndLengthSuite) TestInvalidEncodedType(c *C) {
	_, _, _, err := parseTypeAndLength(
		[]byte{byte(mysql_proto.FieldType_NEWDECIMAL), 123, 'f', 'o', 'o'})

	c.Check(err, Not(IsNil))
}
