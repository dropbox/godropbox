// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

func Test(t *testing.T) {
}

type SqlTypesSuite struct {
}

var _ = Suite(&SqlTypesSuite{})

func (s *SqlTypesSuite) TestNull(c *C) {
	n := Value{}
	c.Assert(n.IsNull(), IsTrue)
	c.Assert(n.String(), Equals, "")

	b := bytes.NewBuffer(nil)
	n.EncodeSql(b)
	c.Assert(b.String(), Equals, "")

	n.EncodeAscii(b)
	c.Assert(b.String(), Equals, "nullnull")
}

func TestNumeric(t *testing.T) {
	n := Value{Numeric([]byte("1234"))}
	b := bytes.NewBuffer(nil)
	n.EncodeSql(b)
	if b.String() != "1234" {
		t.Errorf("Expecting 1234, got %s", b.String())
	}
	n.EncodeAscii(b)
	if b.String() != "12341234" {
		t.Errorf("Expecting 12341234, got %s", b.String())
	}
}

func TestTime(t *testing.T) {
	date := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)
	v, _ := BuildValue(date)
	if v.String() != "1999-01-02 03:04:05.000000000" {
		t.Errorf("Expecting 1999-01-02 03:04:05.000000000, got %s", v.String())
	}

	b := &bytes.Buffer{}
	v.EncodeSql(b)
	if b.String() != "'1999-01-02 03:04:05.000000000'" {
		t.Errorf("Expecting '1999-01-02 03:04:05.000000000', got %s", b.String())
	}
}

const (
	INVALIDNEG = "-9223372036854775809"
	MINNEG     = "-9223372036854775808"
	MAXPOS     = "18446744073709551615"
	INVALIDPOS = "18446744073709551616"
	NEGFLOAT   = "1.234"
	POSFLOAT   = "-1.234"
)

func TestBuildNumeric(t *testing.T) {
	var n Value
	var err error
	n, err = BuildNumeric(MINNEG)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if n.String() != MINNEG {
		t.Errorf("Expecting %v, received %s", MINNEG, n.Raw())
	}
	n, err = BuildNumeric(MAXPOS)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if n.String() != MAXPOS {
		t.Errorf("Expecting %v, received %s", MAXPOS, n.Raw())
	}
	n, err = BuildNumeric("0xA")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if n.String() != "10" {
		t.Errorf("Expecting %v, received %s", 10, n.Raw())
	}
	n, err = BuildNumeric("012")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if string(n.Raw()) != "10" {
		t.Errorf("Expecting %v, received %s", 10, n.Raw())
	}
	if n, err = BuildNumeric(INVALIDNEG); err == nil {
		t.Errorf("Expecting error")
	}
	if n, err = BuildNumeric(INVALIDPOS); err == nil {
		t.Errorf("Expecting error")
	}
	if n, err = BuildNumeric(NEGFLOAT); err == nil {
		t.Errorf("Expecting error")
	}
	if n, err = BuildNumeric(POSFLOAT); err == nil {
		t.Errorf("Expecting error")
	}
}

const (
	HARDSQL               = "\x00'\"\b\n\r\t\x1A\\"
	HARDESCAPED           = "X'002722080a0d091a5c'"
	HARDASCII             = "'ACciCAoNCRpc'"
	PRINTABLE             = "workin' hard"
	PRINTABLE_ESCAPED     = "'workin\\' hard'"
	PRINTABLE_ASCII       = "'d29ya2luJyBoYXJk'"
	SPECIAL_CASES         = "\\ \\_ \\\\ \\\\_ \\% \\\\%"
	SPECIAL_CASES_ESCAPED = "'\\\\ \\_ \\\\\\\\ \\\\\\_ \\% \\\\\\%'"
)

func TestString(t *testing.T) {
	s := MakeString([]byte(HARDSQL))
	b := bytes.NewBuffer(nil)
	s.EncodeSql(b)
	if b.String() != HARDESCAPED {
		t.Errorf("Expecting %s, received %s", HARDESCAPED, b.String())
	}
	b = bytes.NewBuffer(nil)
	s.EncodeAscii(b)
	if b.String() != HARDASCII {
		t.Errorf("Expecting %s, received %#v", HARDASCII, b.String())
	}
	s = MakeString([]byte("abcd"))

	// Now, just printable strings.
	s, err := BuildValue(PRINTABLE)
	if err != nil {
		t.Errorf("BuildValue failed on printable: %s", PRINTABLE)
	}
	b = bytes.NewBuffer(nil)
	s.EncodeSql(b)
	if b.String() != PRINTABLE_ESCAPED {
		t.Errorf("Expecting %s, received %s", PRINTABLE_ESCAPED, b.String())
	}
	b = bytes.NewBuffer(nil)
	s.EncodeAscii(b)
	if b.String() != PRINTABLE_ASCII {
		t.Errorf("Expecting %s, received %#v", PRINTABLE_ASCII, b.String())
	}

	s, err = BuildValue(SPECIAL_CASES)
	if err != nil {
		t.Errorf("BuildValue failed on special cases: %s", SPECIAL_CASES)
	}
	b = bytes.NewBuffer(nil)
	s.EncodeSql(b)
	if b.String() != SPECIAL_CASES_ESCAPED {
		t.Errorf("Expecting %s, received %s", SPECIAL_CASES_ESCAPED, b.String())
	}
}

func (s *SqlTypesSuite) TestBuildValue(c *C) {
	v, err := BuildValue(nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)

	var n64 uint64
	err = ConvertAssign(v, &n64)
	c.Assert(err, NotNil)

	v, err = BuildValue(int(-1))
	c.Assert(err, IsNil)
	c.Assert(v.IsNumeric(), IsTrue)
	c.Assert(v.String(), Equals, "-1")

	v, err = BuildValue(int32(-1))
	c.Assert(err, IsNil)
	c.Assert(v.IsNumeric(), IsTrue)
	c.Assert(v.String(), Equals, "-1")

	v, err = BuildValue(int64(-1))
	c.Assert(err, IsNil)
	c.Assert(v.IsNumeric(), IsTrue)
	c.Assert(v.String(), Equals, "-1")

	err = ConvertAssign(v, &n64)
	c.Assert(err, NotNil)

	v, err = BuildValue(uint(1))
	c.Assert(err, IsNil)
	c.Assert(v.IsNumeric(), IsTrue)
	c.Assert(v.String(), Equals, "1")

	v, err = BuildValue(uint32(1))
	c.Assert(err, IsNil)
	c.Assert(v.IsNumeric(), IsTrue)
	c.Assert(v.String(), Equals, "1")

	v, err = BuildValue(uint64(1))
	c.Assert(err, IsNil)

	err = ConvertAssign(v, &n64)
	c.Assert(err, IsNil)
	c.Assert(n64, Equals, int64(1))
	c.Assert(v.IsNumeric(), IsTrue)
	c.Assert(v.String(), Equals, "1")

	v, err = BuildValue(1.23)
	c.Assert(err, IsNil)
	c.Assert(v.IsFractional(), IsTrue)
	c.Assert(v.String(), Equals, "1.23")

	err = ConvertAssign(v, &n64)
	c.Assert(err, NotNil)

	v, err = BuildValue("abcd")
	c.Assert(err, IsNil)
	c.Assert(v.IsString(), IsTrue)
	c.Assert(v.String(), Equals, "abcd")

	v, err = BuildValue([]byte("abcd"))
	c.Assert(err, IsNil)
	c.Assert(v.IsString(), IsTrue)
	c.Assert(v.String(), Equals, "abcd")

	err = ConvertAssign(v, &n64)
	c.Assert(err, NotNil)

	v, err = BuildValue(time.Date(2012, time.February, 24, 23, 19, 43, 10, time.UTC))
	c.Assert(err, IsNil)
	c.Assert(v.IsString(), IsTrue)
	c.Assert(v.String(), Equals, "2012-02-24 23:19:43")

	v, err = BuildValue(Numeric([]byte("123")))
	c.Assert(err, IsNil)
	c.Assert(v.IsNumeric(), IsTrue)
	c.Assert(v.String(), Equals, "123")

	v, err = BuildValue(Fractional([]byte("12.3")))
	c.Assert(err, IsNil)
	c.Assert(v.IsFractional(), IsTrue)
	c.Assert(v.String(), Equals, "12.3")

	v, err = BuildValue(String{data: []byte("abc")})
	c.Assert(err, IsNil)
	c.Assert(v.IsString(), IsTrue)
	c.Assert(v.String(), Equals, "abc")

	v, err = BuildValue(float32(1.23))
	c.Assert(err, NotNil)

	v1 := MakeString([]byte("ab"))
	v, err = BuildValue(v1)
	c.Assert(err, IsNil)
	c.Assert(v.IsString(), IsTrue)
	c.Assert(v.String(), Equals, "ab")

	v, err = BuildValue(float32(1.23))
	c.Assert(err, NotNil)
}

func TestConvertAssignDefault(t *testing.T) {
	v, err := BuildValue(3)
	if err != nil {
		t.Errorf("%v", err)
	}

	var dst uint64
	err = ConvertAssignDefault(v, &dst, 0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if dst != 3 {
		t.Errorf("Unexpected value %d; expected 3", dst)
	}

	v, err = BuildValue(nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	err = ConvertAssignDefault(v, &dst, 0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if dst != 0 {
		t.Errorf("Unexpected value %d; expected 0", dst)
	}
}

func TestConvertAssignRow(t *testing.T) {
	var err error

	row := make([]Value, 4, 4)
	row[0], err = BuildValue(int(123))
	if err != nil {
		t.Errorf("%v", err)
	}
	row[1], err = BuildValue(int64(-1))
	if err != nil {
		t.Errorf("%v", err)
	}
	row[2], err = BuildValue("abcd")
	if err != nil {
		t.Errorf("%v", err)
	}

	row[3], err = BuildValue([]byte("fdsa"))
	if err != nil {
		t.Errorf("%v", err)
	}

	var n32 int
	var n64 int64
	var str string
	var buffer []byte

	err = ConvertAssignRow(row, &n32, &n64, &str, &buffer)
	if err != nil {
		t.Errorf("%v", err)
	}
	if n32 != 123 {
		t.Errorf("Expecting 123")
	}
	if n64 != -1 {
		t.Errorf("Expecting -1")
	}
	if str != "abcd" {
		t.Errorf("Expecting abcd")
	}
	if !bytes.Equal(buffer, []byte("fdsa")) {
		t.Errorf("Expecting fdsa")
	}
}

func TestConvertAssignRowLengthMismatch(t *testing.T) {
	var err error

	row := make([]Value, 4, 4)
	row[0], err = BuildValue(int(123))
	if err != nil {
		t.Errorf("%v", err)
	}
	row[1], err = BuildValue(int64(-1))
	if err != nil {
		t.Errorf("%v", err)
	}
	row[2], err = BuildValue("abcd")
	if err != nil {
		t.Errorf("%v", err)
	}

	row[3], err = BuildValue([]byte("fdsa"))
	if err != nil {
		t.Errorf("%v", err)
	}

	var n32 int
	var n64 int64
	var str string

	err = ConvertAssignRow(row, &n32, &n64, &str)
	if err == nil {
		t.Errorf("Expecting error")
	}
}

// Ensure DONTESCAPE is not escaped
func TestEncode(t *testing.T) {
	if SqlEncodeMap[DONTESCAPE] != DONTESCAPE {
		t.Errorf("Encode fail: %v", SqlEncodeMap[DONTESCAPE])
	}
	if SqlDecodeMap[DONTESCAPE] != DONTESCAPE {
		t.Errorf("Decode fail: %v", SqlDecodeMap[DONTESCAPE])
	}
}

func (v Value) Generate(rand *rand.Rand, size int) reflect.Value {
	buildFunc := func(goval interface{}) Value {
		v, _ := BuildValue(goval)
		return v
	}

	switch rand.Intn(5) {
	case 0:
		return reflect.ValueOf(buildFunc(nil))
	case 1:
		return reflect.ValueOf(buildFunc(rand.Int()))
	case 2:
		return reflect.ValueOf(buildFunc(rand.NormFloat64()))
	case 3:
		return reflect.ValueOf(buildFunc("string"))
	case 4:
		return reflect.ValueOf(buildFunc([]byte("[]byte")))
	}

	return reflect.ValueOf(buildFunc(nil))
}

func TestMarshalUnmarshalBinary(t *testing.T) {
	f := func(v Value) bool {
		data, err := v.MarshalBinary()
		if err != nil {
			return false
		}

		var v2 Value
		err = v2.UnmarshalBinary(data)
		if err != nil {
			return false
		}

		return reflect.DeepEqual(v, v2)
	}

	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
