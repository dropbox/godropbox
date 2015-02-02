package binlog

import (
	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// This contains field descriptors for bit types as defined by sql/field.h
// In particular:
//
// Field (abstract)
// |
// +--Field_bit
// |  +--Field_bit_as_char
// ...
//
// NOTE: The Field_bit/Field_bit_as_char inheritance relationship is
// broken/backwards since Field_bit is an extended version of
// Field_bit_as_char and not the other way around.

type bitFieldDescriptor struct {
	baseFieldDescriptor

	numBits uint16
}

// This returns a field descriptor for FieldType_BIT (i.e., Field_bit_as_char)
func NewBitFieldDescriptor(nullable NullableColumn, metadata []byte) (
	fd FieldDescriptor,
	remaining []byte,
	err error) {

	if len(metadata) < 2 {
		return nil, nil, errors.New("Metadata has too few bytes")
	}

	return &bitFieldDescriptor{
		baseFieldDescriptor: baseFieldDescriptor{
			fieldType:  mysql_proto.FieldType_BIT,
			isNullable: nullable,
		},
		numBits: ((uint16(metadata[0]) * 8) + uint16(metadata[1])),
	}, metadata[2:], nil
}

func (d *bitFieldDescriptor) ParseValue(data []byte) (
	value interface{},
	remaining []byte,
	err error) {

	return nil, nil, errors.New("TODO")
}
