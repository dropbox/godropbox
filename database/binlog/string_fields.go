package binlog

import (
	"bytes"
	"math"

	"github.com/dropbox/godropbox/errors"
	mysql_proto "github.com/dropbox/godropbox/proto/mysql"
)

// This continas field descriptors for string types as defined by sql/field.h.
// In particular:
//
// Field (abstract)
// |
// ...
// |
// +--Field_str (abstract)
// |  +--Field_longstr
// |  |  +--Field_string
// |  |  +--Field_varstring
// |  |  +--Field_blob
// |  |     +--Field_geom
// |  |
// |  +--Field_null
// |  +--Field_enum
// |     +--Field_set
// ...

// This is used for extracting type / length info from string field's metadata.
func parseTypeAndLength(metadata []byte) (
	fieldType mysql_proto.FieldType_Type,
	length int,
	remaining []byte,
	err error) {

	if len(metadata) < 2 {
		return mysql_proto.FieldType_STRING, 0, nil, errors.New(
			"not enough metadata bytes")
	}

	byte1 := int(metadata[0])
	byte2 := int(metadata[1])

	var realType mysql_proto.FieldType_Type
	if (byte1 & 0x30) != 0x30 { // see mysql issue #37426
		realType = mysql_proto.FieldType_Type(byte1 | 0x30)
		length = byte2 | (((byte1 & 0x30) ^ 0x30) << 4)
	} else {
		realType = mysql_proto.FieldType_Type(byte1)
		length = byte2
	}

	if realType != mysql_proto.FieldType_SET &&
		realType != mysql_proto.FieldType_ENUM &&
		realType != mysql_proto.FieldType_STRING &&
		realType != mysql_proto.FieldType_VAR_STRING {

		return mysql_proto.FieldType_STRING, 0, nil, errors.Newf(
			"Invalid real type: %s (%d)",
			realType.String(),
			realType)
	}

	return realType, length, metadata[2:], nil
}

// This returns a field descriptor for FieldType_NULL (i.e., Field_null)
func NewNullFieldDescriptor(nullable NullableColumn) FieldDescriptor {
	// A null field can be nullable ...
	return newFixedLengthFieldDescriptor(
		mysql_proto.FieldType_NULL,
		nullable,
		0,
		func(b []byte) interface{} { return nil })
}

//
// packedLengthFieldDescriptor ------------------------------------------------
//

type packedLengthFieldDescriptor struct {
	baseFieldDescriptor

	packedLength int
}

func (d *packedLengthFieldDescriptor) parseValue(data []byte) (
	value interface{},
	remaining []byte,
	err error) {

	sizeBytes, remaining, err := readSlice(data, d.packedLength)
	if err != nil {
		return nil, nil, err
	}

	size := bytesToLEUint(sizeBytes)

	// NOTE: slice and long blob don't work well together.  In particular,
	// the largest slice is 2GB (because the len intrinsic returns an int),
	// while the largest long blob is 4GB.
	if size > uint64(math.MaxInt32) {
		return nil, nil, errors.Newf("Blob too large: %d", size)
	}

	return readSlice(remaining, int(size))
}

//
// stringFieldDescriptor -----------------------------------------------------
//

type stringFieldDescriptor struct {
	packedLengthFieldDescriptor

	maxLength int
}

// This returns a field descriptor for FieldType_VARCHAR (i.e., Field_varstring)
func NewVarcharFieldDescriptor(nullable NullableColumn, metadata []byte) (
	fd FieldDescriptor,
	remaining []byte,
	err error) {

	if len(metadata) < 2 {
		return nil, nil, errors.New("Metadata has too few bytes")
	}

	maxLen := int(LittleEndian.Uint16(metadata))

	return NewStringFieldDescriptor(
		mysql_proto.FieldType_VARCHAR,
		nullable,
		maxLen), metadata[2:], nil
}

func NewStringFieldDescriptor(
	fieldType mysql_proto.FieldType_Type,
	nullable NullableColumn,
	maxLen int) FieldDescriptor {

	packedLen := 2
	if maxLen < 256 {
		packedLen = 1
	}

	return &stringFieldDescriptor{
		packedLengthFieldDescriptor: packedLengthFieldDescriptor{
			baseFieldDescriptor: baseFieldDescriptor{
				fieldType:  fieldType,
				isNullable: nullable,
			},
			packedLength: packedLen,
		},
		maxLength: maxLen,
	}
}

func (d *stringFieldDescriptor) ParseValue(data []byte) (
	value interface{},
	remaining []byte,
	err error) {

	value, remaining, err = d.parseValue(data)
	if d.fieldType != mysql_proto.FieldType_STRING || err != nil {
		return value, remaining, err
	}

	bytesValue, ok := value.([]byte)
	if !ok {
		return value, remaining, nil
	}

	if len(bytesValue) < d.maxLength {
		// NOTE: We have to allocate a new copy instead of padding it in place
		// since it is a slice pointing to same backing array as remaining.
		newBytesValue := bytes.Repeat([]byte("\x00"), d.maxLength)
		copy(newBytesValue, bytesValue)
		bytesValue = newBytesValue
	}

	return bytesValue, remaining, nil
}

//
// blobFieldDescriptor --------------------------------------------------------
//

type blobFieldDescriptor struct {
	packedLengthFieldDescriptor
}

// This returns a field descriptor for FieldType_BLOB (i.e., Field_blob)
func NewBlobFieldDescriptor(nullable NullableColumn, metadata []byte) (
	fd FieldDescriptor,
	remaining []byte,
	err error) {

	if len(metadata) < 1 {
		return nil, nil, errors.New("Metadata has too few bytes")
	}

	packedLen := LittleEndian.Uint8(metadata)

	if packedLen > 4 {
		return nil, nil, errors.New("Invalid packed length")
	}

	return &blobFieldDescriptor{
		packedLengthFieldDescriptor: packedLengthFieldDescriptor{
			baseFieldDescriptor: baseFieldDescriptor{
				fieldType:  mysql_proto.FieldType_BLOB,
				isNullable: nullable,
			},
			packedLength: int(packedLen),
		},
	}, metadata[1:], nil
}

func (d *blobFieldDescriptor) ParseValue(data []byte) (
	value interface{},
	remaining []byte,
	err error) {

	return d.parseValue(data)
}
