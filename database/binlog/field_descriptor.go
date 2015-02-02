package binlog

import (
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

type NullableColumn bool

const (
	Nullable    NullableColumn = true
	NotNullable NullableColumn = false
)

// FieldDescriptor defines the common interface for interpreting all mysql
// field types.
type FieldDescriptor interface {
	// Type returns the descriptor's field type.
	Type() mysql_proto.FieldType_Type

	// IsNullable returns whether or not the field is nullable.
	IsNullable() bool

	// ParseValue extracts a single mysql value from the data array.  The value
	// must an uint64 for int fields (NOTE that sign is uninterpreted), double
	// for floating point fields, []byte for string fields, and time.Time
	// (in UTC) for temporal fields.
	ParseValue(data []byte) (value interface{}, remaining []byte, err error)
}

// NOTE: ParseValue is left unimplemented.
type baseFieldDescriptor struct {
	fieldType  mysql_proto.FieldType_Type
	isNullable NullableColumn
}

func (d *baseFieldDescriptor) Type() mysql_proto.FieldType_Type {
	return d.fieldType
}

func (d *baseFieldDescriptor) IsNullable() bool {
	return d.isNullable == Nullable
}

type ColumnDescriptor interface {
	FieldDescriptor

	// IndexPosition returns the column's table index position.
	IndexPosition() int
}

type columnDescriptorImpl struct {
	FieldDescriptor

	index int
}

func NewColumnDescriptor(fd FieldDescriptor, pos int) ColumnDescriptor {
	return &columnDescriptorImpl{
		FieldDescriptor: fd,
		index:           pos,
	}
}

func (c *columnDescriptorImpl) IndexPosition() int {
	return c.index
}

//
// fixedLengthFieldDescriptor -------------------------------------------------
//

// Generic fixed length field descriptor
type fixedLengthFieldDescriptor struct {
	baseFieldDescriptor

	numBytes  int
	parseFunc func([]byte) interface{}
}

func newFixedLengthFieldDescriptor(
	fieldType mysql_proto.FieldType_Type,
	nullable NullableColumn,
	numBytes int,
	parseFunc func([]byte) interface{}) FieldDescriptor {

	return &fixedLengthFieldDescriptor{
		baseFieldDescriptor: baseFieldDescriptor{
			fieldType:  fieldType,
			isNullable: nullable,
		},
		numBytes:  numBytes,
		parseFunc: parseFunc,
	}
}

func (d *fixedLengthFieldDescriptor) ParseValue(data []byte) (
	value interface{},
	remaining []byte,
	err error) {

	data, remaining, err = readSlice(data, d.numBytes)
	if err != nil {
		return nil, remaining, err
	}

	return d.parseFunc(data), remaining, nil
}
