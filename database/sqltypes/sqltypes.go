// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

// Package sqltypes implements interfaces and types that represent SQL values.
//
// DROPBOX NOTE: This is a modified version of vitess's sqltypes module.
// The original source can be found at https://code.google.com/p/vitess/
package sqltypes

import (
    "encoding/base64"
    "encoding/json"
    "reflect"
    "strconv"
    "time"

    "dropbox/util/encoding2"
    "godropbox/errors"
)

var (
    NULL       = Value{}
    DONTESCAPE = byte(255)
    nullstr    = []byte("null")
)

// Value can store any SQL value. NULL is stored as nil.
type Value struct {
    Inner InnerValue
}

// Numeric represents non-fractional SQL number.
type Numeric []byte

// Fractional represents fractional types like float and decimal
// It's functionally equivalent to Numeric other than how it's constructed
type Fractional []byte

// String represents any SQL type that needs to be represented using quotes.
// If isUtf8 is false, it will be hex encoded so it's safe for exception reporting, etc.
type String struct {
    data   []byte
    isUtf8 bool
}

// MakeNumeric makes a Numeric from a []byte without validation.
func MakeNumeric(b []byte) Value {
    return Value{Numeric(b)}
}

// MakeFractional makes a Fractional value from a []byte without validation.
func MakeFractional(b []byte) Value {
    return Value{Fractional(b)}
}

// MakeString makes a String value from a []byte.
func MakeString(b []byte) Value {
    return Value{String{b, false}}
}

// Raw returns the raw bytes. All types are currently implemented as []byte.
func (v Value) Raw() []byte {
    if v.Inner == nil {
        return nil
    }
    return v.Inner.raw()
}

// String returns the raw value as a string
func (v Value) String() string {
    if v.Inner == nil {
        return ""
    }
    return string(v.Inner.raw())
}

// EncodeSql encodes the value into an SQL statement. Can be binary.
func (v Value) EncodeSql(b encoding2.BinaryWriter) {
    if v.Inner == nil {
        if _, err := b.Write(nullstr); err != nil {
            panic(err)
        }
    } else {
        v.Inner.encodeSql(b)
    }
}

// EncodeAscii encodes the value using 7-bit clean ascii bytes.
func (v Value) EncodeAscii(b encoding2.BinaryWriter) {
    if v.Inner == nil {
        if _, err := b.Write(nullstr); err != nil {
            panic(err)
        }
    } else {
        v.Inner.encodeAscii(b)
    }
}

func (v Value) IsNull() bool {
    return v.Inner == nil
}

func (v Value) IsNumeric() (ok bool) {
    _ = Numeric(nil) // compiler bug work-around
    if v.Inner != nil {
        _, ok = v.Inner.(Numeric)
    }
    return ok
}

func (v Value) IsFractional() (ok bool) {
    _ = Fractional(nil) // compiler bug work-around
    if v.Inner != nil {
        _, ok = v.Inner.(Fractional)
    }
    return ok
}

func (v Value) IsString() (ok bool) {
    _ = String{} // compiler bug work-around
    if v.Inner != nil {
        _, ok = v.Inner.(String)
    }
    return ok
}

func (v Value) MarshalJSON() ([]byte, error) {
    if v.IsString() {
        return json.Marshal(v.Inner.(String).data)
    }
    return json.Marshal(v.Inner)
}

// InnerValue defines methods that need to be supported by all non-null value types.
type InnerValue interface {
    raw() []byte
    encodeSql(encoding2.BinaryWriter)
    encodeAscii(encoding2.BinaryWriter)
}

func BuildValue(goval interface{}) (v Value, err error) {
    switch bindVal := goval.(type) {
    case nil:
        // no op
    case bool:
        val := 0
        if bindVal {
            val = 1
        }
        v = Value{Numeric(strconv.AppendInt(nil, int64(val), 10))}
    case int:
        v = Value{Numeric(strconv.AppendInt(nil, int64(bindVal), 10))}
    case int32:
        v = Value{Numeric(strconv.AppendInt(nil, int64(bindVal), 10))}
    case int64:
        v = Value{Numeric(strconv.AppendInt(nil, int64(bindVal), 10))}
    case uint:
        v = Value{Numeric(strconv.AppendUint(nil, uint64(bindVal), 10))}
    case uint8:
        v = Value{Numeric(strconv.AppendUint(nil, uint64(bindVal), 10))}
    case uint32:
        v = Value{Numeric(strconv.AppendUint(nil, uint64(bindVal), 10))}
    case uint64:
        v = Value{Numeric(strconv.AppendUint(nil, uint64(bindVal), 10))}
    case float64:
        v = Value{Fractional(strconv.AppendFloat(nil, bindVal, 'f', -1, 64))}
    case string:
        v = Value{String{[]byte(bindVal), true}}
    case []byte:
        v = Value{String{bindVal, false}}
    case time.Time:
        v = Value{String{[]byte(bindVal.Format("'2006-01-02 15:04:05'")), true}}
    case Numeric, Fractional, String:
        v = Value{bindVal.(InnerValue)}
    case Value:
        v = bindVal
    default:
        return Value{}, errors.Newf("Unsupported bind variable type %T: %v", goval, goval)
    }
    return v, nil
}

// ConvertAssignRow copies a row of values in the list of destinations.  An
// error is returned if any one of the row's element coping is done between
// incompatible value and dest types.  The list of destinations must contain
// pointers.
// Note that for anything else than *[]byte the value is copied, however if
// the destination is of type *[]byte it will point the same []byte array as
// the source (no copying).
func ConvertAssignRow(row []Value, dest ...interface{}) error {
    if len(row) != len(dest) {
        return errors.Newf(
            "# of row entries %d does not match # of destinations %d",
            len(row),
            len(dest))
    }

    for i := 0; i < len(row); i++ {
        err := ConvertAssign(row[i], dest[i])
        if err != nil {
            return err
        }
    }

    return nil
}

// ConvertAssign copies to the '*dest' the value in 'src'. An error is returned
// if the coping is done between incompatible Value and dest types. 'dest' must be
// a pointer type.
// Note that for anything else than *[]byte the value is copied, however if 'dest'
// is of type *[]byte it will point to same []byte array as 'src.Raw()' (no copying).
func ConvertAssign(src Value, dest interface{}) error {
    // TODO(zviad): reflecting might be too slow so common cases
    // can probably be handled without reflections
    var s String
    var n Numeric
    var f Fractional
    var ok bool
    var err error

    if src.Inner == nil {
        return errors.Newf("source is null")
    }

    switch d := dest.(type) {
    case *string:
        if s, ok = src.Inner.(String); !ok {
            return errors.Newf("source: '%v' is not String", src)
        }
        *d = string(s.raw())
        return nil
    case *[]byte:
        if s, ok = src.Inner.(String); !ok {
            return errors.Newf("source: '%v' is not String", src)
        }
        *d = s.raw()
        return nil
        // TODO(zviad): figure out how to do this without reflections
        // because I think reflections are slow?
        //case *int, *int8, *int16, *int32, *int64:
        //    if n, ok := src.Inner.(Numeric); !ok {
        //        return errors.Newf("source: %v is not Numeric", src)
        //    }
        //	if i64, err := strconv.ParseInt(string(n.raw()), 10, 64); err != nil {
        //		return err
        //	}
        //    *d = i64
        //	return nil
    }

    dpv := reflect.ValueOf(dest)
    if dpv.Kind() != reflect.Ptr {
        return errors.Newf("destination not a pointer")
    }
    if dpv.IsNil() {
        return errors.Newf("destination pointer is Nil")
    }
    dv := reflect.Indirect(dpv)
    switch dv.Kind() {
    case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
        if n, ok = src.Inner.(Numeric); !ok {
            return errors.Newf("source: '%v' is not Numeric", src)
        }
        var i64 int64
        if i64, err = strconv.ParseInt(string(n.raw()), 10, dv.Type().Bits()); err != nil {
            return err
        }
        dv.SetInt(i64)
        return nil
    case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
        if n, ok = src.Inner.(Numeric); !ok {
            return errors.Newf("source: '%v' is not Numeric", src)
        }
        var u64 uint64
        if u64, err = strconv.ParseUint(string(n.raw()), 10, dv.Type().Bits()); err != nil {
            return err
        }
        dv.SetUint(u64)
        return nil
    case reflect.Float32, reflect.Float64:
        if f, ok = src.Inner.(Fractional); !ok {
            return errors.Newf("source: '%v' is not Fractional", src)
        }
        var f64 float64
        if f64, err = strconv.ParseFloat(string(f.raw()), dv.Type().Bits()); err != nil {
            return err
        }
        dv.SetFloat(f64)
        return nil

    case reflect.Bool:
        // treat bool as true if non-zero integer
        if n, ok = src.Inner.(Numeric); !ok {
            return errors.Newf("source: '%v' is not Numeric", src)
        }
        var i64 int64
        if i64, err = strconv.ParseInt(string(n.raw()), 10, 64); err != nil {
            return err
        }
        dv.SetBool(i64 != 0)
        return nil
    }

    return errors.Newf("unsupported destination type: %v", dest)
}

// ConvertAssign, but with support for default values
func ConvertAssignDefault(src Value, dest interface{}, defaultValue interface{}) error {
    if src.IsNull() {
        // This is not the most efficient way of doing things, but it's certainly cleaner
        v, err := BuildValue(defaultValue)
        if err != nil {
            return err
        }
        return ConvertAssign(v, dest)
    }
    return ConvertAssign(src, dest)
}

// BuildNumeric builds a Numeric type that represents any whole number.
// It normalizes the representation to ensure 1:1 mapping between the
// number and its representation.
func BuildNumeric(val string) (n Value, err error) {
    if val[0] == '-' || val[0] == '+' {
        signed, err := strconv.ParseInt(val, 0, 64)
        if err != nil {
            return Value{}, err
        }
        n = Value{Numeric(strconv.AppendInt(nil, signed, 10))}
    } else {
        unsigned, err := strconv.ParseUint(val, 0, 64)
        if err != nil {
            return Value{}, err
        }
        n = Value{Numeric(strconv.AppendUint(nil, unsigned, 10))}
    }
    return n, nil
}

func (n Numeric) raw() []byte {
    return []byte(n)
}

func (n Numeric) encodeSql(b encoding2.BinaryWriter) {
    if _, err := b.Write(n.raw()); err != nil {
        panic(err)
    }
}

func (n Numeric) encodeAscii(b encoding2.BinaryWriter) {
    if _, err := b.Write(n.raw()); err != nil {
        panic(err)
    }
}

func (n Numeric) MarshalJSON() ([]byte, error) {
    return n.raw(), nil
}

func (f Fractional) raw() []byte {
    return []byte(f)
}

func (f Fractional) encodeSql(b encoding2.BinaryWriter) {
    if _, err := b.Write(f.raw()); err != nil {
        panic(err)
    }
}

func (f Fractional) encodeAscii(b encoding2.BinaryWriter) {
    if _, err := b.Write(f.raw()); err != nil {
        panic(err)
    }
}

func (s String) raw() []byte {
    return []byte(s.data)
}

func (s String) encodeSql(b encoding2.BinaryWriter) {
    if s.isUtf8 {
        writebyte(b, '\'')
        for _, ch := range s.raw() {
            if encodedChar := SqlEncodeMap[ch]; encodedChar == DONTESCAPE {
                writebyte(b, ch)
            } else {
                writebyte(b, '\\')
                writebyte(b, encodedChar)
            }
        }
        writebyte(b, '\'')
    } else {
        b.Write([]byte("X'"))
        encoding2.HexEncodeToWriter(b, s.raw())
        writebyte(b, '\'')
    }
}

func (s String) encodeAscii(b encoding2.BinaryWriter) {
    writebyte(b, '\'')
    encoder := base64.NewEncoder(base64.StdEncoding, b)
    encoder.Write(s.raw())
    encoder.Close()
    writebyte(b, '\'')
}

func writebyte(b encoding2.BinaryWriter, c byte) {
    if err := b.WriteByte(c); err != nil {
        panic(err)
    }
}

// Helper function for converting a uint64 to a string suitable for SQL.
func Uint64EncodeSql(b encoding2.BinaryWriter, num uint64) {
    numVal, _ := BuildValue(num)
    numVal.EncodeSql(b)
}

// SqlEncodeMap specifies how to escape binary data with '\'.
// Complies to http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
var SqlEncodeMap [256]byte

// SqlDecodeMap is the reverse of SqlEncodeMap
var SqlDecodeMap [256]byte

var encodeRef = map[byte]byte{
    '\x00': '0',
    '\'':   '\'',
    '"':    '"',
    '\b':   'b',
    '\n':   'n',
    '\r':   'r',
    '\t':   't',
    26:     'Z', // ctl-Z
    '\\':   '\\',
}

func init() {
    for i, _ := range SqlEncodeMap {
        SqlEncodeMap[i] = DONTESCAPE
        SqlDecodeMap[i] = DONTESCAPE
    }
    for i, _ := range SqlEncodeMap {
        if to, ok := encodeRef[byte(i)]; ok {
            SqlEncodeMap[byte(i)] = to
            SqlDecodeMap[to] = byte(i)
        }
    }
}
