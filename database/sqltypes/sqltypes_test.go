// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
    "bytes"
    "testing"
    "time"

    "dropbox/util/testing2"
)

func TestNull(t *testing.T) {
    n := Value{}
    if !n.IsNull() {
        t.Errorf("value is not null")
    }
    if n.String() != "" {
        t.Errorf("Expecting '', got %s", n.String())
    }
    b := bytes.NewBuffer(nil)
    n.EncodeSql(b)
    if b.String() != "null" {
        t.Errorf("Expecting null, got %s", b.String())
    }
    n.EncodeAscii(b)
    if b.String() != "nullnull" {
        t.Errorf("Expecting nullnull, got %s", b.String())
    }
    js, err := n.MarshalJSON()
    if err != nil {
        t.Errorf("Unexpected error: %s", err)
    }
    if string(js) != "null" {
        t.Errorf("Expecting null, received %s", js)
    }
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
    js, err := n.MarshalJSON()
    if err != nil {
        t.Errorf("Unexpected error: %s", err)
    }
    if string(js) != "1234" {
        t.Errorf("Expecting 1234, received %s", js)
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
    HARDSQL           = "\x00'\"\b\n\r\t\x1A\\"
    HARDESCAPED       = "X'002722080a0d091a5c'"
    HARDASCII         = "'ACciCAoNCRpc'"
    PRINTABLE         = "workin' hard"
    PRINTABLE_ESCAPED = "'workin\\' hard'"
    PRINTABLE_ASCII   = "'d29ya2luJyBoYXJk'"
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
    js, err := s.MarshalJSON()
    if err != nil {
        t.Errorf("Unexpected error: %s", err)
    }
    if string(js) != "\"YWJjZA==\"" {
        t.Errorf("Expecting \"YWJjZA==\", received %s", js)
    }

    // Now, just printable strings.
    s, err = BuildValue(PRINTABLE)
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
}

func TestBuildValue(t *testing.T) {
    h := testing2.H{t}

    v, err := BuildValue(nil)
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsNull() {
        t.Errorf("Expecting null")
    }
    var n64 uint64
    err = ConvertAssign(v, &n64)
    h.AssertErrorContains(err, "source is null")
    v, err = BuildValue(int(-1))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsNumeric() || v.String() != "-1" {
        t.Errorf("Expecting -1, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(int32(-1))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsNumeric() || v.String() != "-1" {
        t.Errorf("Expecting -1, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(int64(-1))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsNumeric() || v.String() != "-1" {
        t.Errorf("Expecting -1, received %T: %s", v.Inner, v.String())
    }
    err = ConvertAssign(v, &n64)
    if err == nil {
        t.Errorf("-1 shouldn't convert into uint64")
    }
    v, err = BuildValue(uint(1))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsNumeric() || v.String() != "1" {
        t.Errorf("Expecting 1, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(uint32(1))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsNumeric() || v.String() != "1" {
        t.Errorf("Expecting 1, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(uint64(1))
    if err != nil {
        t.Errorf("%v", err)
    }
    err = ConvertAssign(v, &n64)
    if err != nil {
        t.Errorf("%v", err)
    }
    if n64 != 1 {
        t.Errorf("Expecting 1, got %v", n64)
    }
    if !v.IsNumeric() || v.String() != "1" {
        t.Errorf("Expecting 1, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(1.23)
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsFractional() || v.String() != "1.23" {
        t.Errorf("Expecting 1.23, received %T: %s", v.Inner, v.String())
    }
    err = ConvertAssign(v, &n64)
    if err == nil {
        t.Errorf("1.23 shouldn't convert into uint64")
    }
    v, err = BuildValue("abcd")
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsString() || v.String() != "abcd" {
        t.Errorf("Expecting abcd, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue([]byte("abcd"))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsString() || v.String() != "abcd" {
        t.Errorf("Expecting abcd, received %T: %s", v.Inner, v.String())
    }
    err = ConvertAssign(v, &n64)
    h.AssertErrorContains(err, "source: 'abcd' is not Numeric")

    v, err = BuildValue(time.Date(2012, time.February, 24, 23, 19, 43, 10, time.UTC))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsString() || v.String() != "'2012-02-24 23:19:43'" {
        t.Errorf("Expecting '2012-02-24 23:19:43', received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(Numeric([]byte("123")))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsNumeric() || v.String() != "123" {
        t.Errorf("Expecting 123, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(Fractional([]byte("12.3")))
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsFractional() || v.String() != "12.3" {
        t.Errorf("Expecting 12.3, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(String{data: []byte("abc")})
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsString() || v.String() != "abc" {
        t.Errorf("Expecting abc, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(float32(1.23))
    if err == nil {
        t.Errorf("Did not receive error")
    }
    v1 := MakeString([]byte("ab"))
    v, err = BuildValue(v1)
    if err != nil {
        t.Errorf("%v", err)
    }
    if !v.IsString() || v.String() != "ab" {
        t.Errorf("Expecting ab, received %T: %s", v.Inner, v.String())
    }
    v, err = BuildValue(float32(1.23))
    if err == nil {
        t.Errorf("Did not receive error")
    }
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
