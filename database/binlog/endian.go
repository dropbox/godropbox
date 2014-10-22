package binlog

import (
	"encoding/binary"
	"math"
)

// Mysql extensions to binary.LittleEndian.
var LittleEndian = littleEndian{binary.LittleEndian}

type littleEndian struct {
	binary.ByteOrder
}

func (littleEndian) Uint8(b []byte) uint8 {
	return uint8(b[0])
}

func (littleEndian) Uint24(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

func (littleEndian) Uint48(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
		uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40
}

func (littleEndian) Float32(b []byte) float32 {
	return math.Float32frombits(LittleEndian.Uint32(b))
}

func (littleEndian) Float64(b []byte) float64 {
	return math.Float64frombits(LittleEndian.Uint64(b))
}

// Mysql extensions to binary.BigEndian.  This is mainly used for decoding
// MyISAM values (as defined in include/myisampack.h).
var BigEndian = bigEndian{binary.BigEndian}

type bigEndian struct {
	binary.ByteOrder
}

func (bigEndian) Uint8(b []byte) uint8 {
	return uint8(b[0])
}

func (bigEndian) Uint24(b []byte) uint32 {
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

func (bigEndian) Uint40(b []byte) uint64 {
	return uint64(b[0])<<32 | uint64(b[1])<<24 | uint64(b[2])<<16 |
		uint64(b[3])<<8 | uint64(b[4])
}

func (bigEndian) Uint48(b []byte) uint64 {
	return uint64(b[0])<<40 | uint64(b[1])<<32 | uint64(b[2])<<24 |
		uint64(b[3])<<16 | uint64(b[4])<<8 | uint64(b[5])
}

func (bigEndian) Uint56(b []byte) uint64 {
	return uint64(b[0])<<48 | uint64(b[1])<<40 | uint64(b[2])<<32 |
		uint64(b[3])<<24 | uint64(b[4])<<16 | uint64(b[5])<<8 |
		uint64(b[6])
}

func (bigEndian) Int8(b []byte) int8 {
	return int8(b[0])
}

func (bigEndian) Int16(b []byte) int16 {
	return int16(BigEndian.Uint16(b))
}

func (bigEndian) Int24(b []byte) int32 {
	val := BigEndian.Uint24(b)
	if int(b[0]) >= 128 { // negative value.
		return int32(val | uint32(255)<<24)
	}
	return int32(val)
}

func (bigEndian) Int32(b []byte) int32 {
	return int32(BigEndian.Uint32(b))
}

func (bigEndian) Int64(b []byte) int64 {
	return int64(BigEndian.Uint64(b))
}
