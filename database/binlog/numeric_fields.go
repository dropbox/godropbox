package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// This contains field descriptors for numeric types as defined by sql/field.h.
// In particular:
//
// Field (abstract)
// |
// ...
// |
// +--Field_num (abstract)
// |  |  +--Field_real (asbstract)
// |  |     +--Field_decimal
// |  |     +--Field_float
// |  |     +--Field_double
// |  |
// |  +--Field_new_decimal
// |  +--Field_short
// |  +--Field_medium
// |  +--Field_long
// |  +--Field_longlong
// |  +--Field_tiny
// ...
//
// NOTE: Field_year is grouped with other temporal fields.

// This returns a field descriptor for FieldType_TINY (i.e., Field_tiny).
func NewTinyFieldDescriptor(nullable NullableColumn) FieldDescriptor {
	return newFixedLengthFieldDescriptor(
		mysql_proto.FieldType_TINY,
		nullable,
		1,
		func(b []byte) interface{} { return uint64(b[0]) })
}

// This returns a field descriptor for FieldType_SHORT (i.e., Field_shart)
func NewShortFieldDescriptor(nullable NullableColumn) FieldDescriptor {
	return newFixedLengthFieldDescriptor(
		mysql_proto.FieldType_SHORT,
		nullable,
		2,
		func(b []byte) interface{} { return uint64(LittleEndian.Uint16(b)) })
}

// This returns a field descriptor for FieldType_INT24 (i.e., Field_medium)
func NewInt24FieldDescriptor(nullable NullableColumn) FieldDescriptor {
	return newFixedLengthFieldDescriptor(
		mysql_proto.FieldType_INT24,
		nullable,
		3,
		func(b []byte) interface{} { return uint64(LittleEndian.Uint24(b)) })
}

// This returns a field descriptor for FieldType_LONG (i.e., Field_long)
func NewLongFieldDescriptor(nullable NullableColumn) FieldDescriptor {
	return newFixedLengthFieldDescriptor(
		mysql_proto.FieldType_LONG,
		nullable,
		4,
		func(b []byte) interface{} { return uint64(LittleEndian.Uint32(b)) })
}

// This returns a field descriptor for FieldType_LONGLONG (i.e., Field_longlong)
func NewLongLongFieldDescriptor(nullable NullableColumn) FieldDescriptor {
	return newFixedLengthFieldDescriptor(
		mysql_proto.FieldType_LONGLONG,
		nullable,
		8,
		func(b []byte) interface{} { return LittleEndian.Uint64(b) })
}

// This returns a field descriptor for FieldType_FLOAT (i.e., Field_float)
func NewFloatFieldDescriptor(nullable NullableColumn, metadata []byte) (
	fd FieldDescriptor,
	remaining []byte,
	err error) {

	if len(metadata) < 1 {
		return nil, nil, errors.New("metadata too short")
	}

	size := uint8(metadata[0])
	if size != 4 {
		return nil, nil, errors.New("invalid float size")
	}

	return newFixedLengthFieldDescriptor(
			mysql_proto.FieldType_FLOAT,
			nullable,
			4,
			func(b []byte) interface{} {
				return float64(LittleEndian.Float32(b))
			}),
		metadata[1:],
		nil
}

// This returns a field descriptor for FieldType_DOUBLE (i.e., Field_double)
func NewDoubleFieldDescriptor(nullable NullableColumn, metadata []byte) (
	fd FieldDescriptor,
	remaining []byte,
	err error) {

	if len(metadata) < 1 {
		return nil, nil, errors.New("metadata too short")
	}

	size := uint8(metadata[0])
	if size != 8 {
		return nil, nil, errors.New("invalid double size")
	}

	return newFixedLengthFieldDescriptor(
			mysql_proto.FieldType_DOUBLE,
			nullable,
			8,
			func(b []byte) interface{} { return LittleEndian.Float64(b) }),
		metadata[1:],
		nil
}

type decimalFieldDescriptor struct {
	baseFieldDescriptor
}

// This returns a field descriptor for FieldType_DECIMAL (i.e., Field_decimal)
func NewDecimalFieldDescriptor(nullable NullableColumn) FieldDescriptor {
	return &decimalFieldDescriptor{
		baseFieldDescriptor: baseFieldDescriptor{
			fieldType:  mysql_proto.FieldType_DECIMAL,
			isNullable: nullable,
		},
	}
}

func (d *decimalFieldDescriptor) ParseValue(data []byte) (
	value interface{},
	remaining []byte,
	err error) {

	return nil, nil, errors.New("TODO")
}

type newDecimalFieldDescriptor struct {
	baseFieldDescriptor

	precision uint8
	decimals  uint8
}

// This returns a field descriptor for FieldType_NEWDECIMAL (i.e.,
// Field_newdecimal)
func NewNewDecimalFieldDescriptor(nullable NullableColumn, metadata []byte) (
	fd FieldDescriptor,
	remaining []byte,
	err error) {

	if len(metadata) < 2 {
		return nil, nil, errors.New("Metadata has too few bytes")
	}

	return &newDecimalFieldDescriptor{
		baseFieldDescriptor: baseFieldDescriptor{
			fieldType:  mysql_proto.FieldType_NEWDECIMAL,
			isNullable: nullable,
		},

		precision: uint8(metadata[0]),
		decimals:  uint8(metadata[1]),
	}, metadata[2:], nil
}

func (d *newDecimalFieldDescriptor) ParseValue(data []byte) (
	value interface{},
	remaining []byte,
	err error) {

	return nil, nil, errors.New("TODO")
}
