package binlog

import (
	"bytes"
	"encoding/binary"

	"github.com/dropbox/godropbox/errors"
)

func bytesToLEUint(valBytes []byte) uint64 {
	val := uint64(0)
	for i, b := range valBytes {
		val += uint64(b) << (uint(i) * 8)
	}
	return val
}

const NullLength = uint64(^uint32(0))

// Note: this is equivalent to net_field_length in sql-common/pack.c
func readFieldLength(valBytes []byte) (
	length uint64,
	remaining []byte,
	err error) {

	if len(valBytes) == 0 {
		return 0, nil, errors.New("Empty field length input")
	}

	val := uint64(valBytes[0])
	if val < 251 {
		return val, valBytes[1:], nil
	} else if val == 251 {
		return NullLength, valBytes[1:], nil
	}
	size := 9
	if val == 252 {
		size = 3
	} else if val == 253 {
		size = 4
	}
	if len(valBytes) < size {
		return 0, nil, errors.Newf(
			"Invalid field length input (expected at least %d bytes)",
			size)
	}
	// NOTE: mysql's net_field_length implementation is somewhat broken.
	// In particular, when net_store_length encode a ulong using 8 bytes,
	// net_field_length will only read the first 4 bytes, and ignore the
	// rest ....
	return bytesToLEUint(valBytes[1:size]), valBytes[size:], nil
}

func readLittleEndian(valBytes []byte, val interface{}) ([]byte, error) {
	r := bytes.NewBuffer(valBytes)
	err := binary.Read(r, binary.LittleEndian, val)
	if err != nil {
		return nil, err
	}
	return r.Bytes(), nil
}

func readSlice(valBytes []byte, n int) (
	slice []byte,
	remaining []byte,
	err error) {

	if len(valBytes) < n {
		return nil, nil, errors.New("not enough bytes")
	}

	return valBytes[:n], valBytes[n:], nil
}

func readBitArray(bytes []byte, numVals int) (
	bits []bool,
	remaining []byte,
	err error) {

	bytesUsed := ((numVals + 7) / 8)

	if len(bytes) < bytesUsed {
		return nil, nil, errors.New("Not enough bytes")
	}

	bitVector := make([]bool, numVals, numVals)
	for i := 0; i < numVals; i++ {
		bitVector[i] = (uint8(bytes[i/8]) & (1 << (uint(i) % 8))) != 0
	}
	return bitVector, bytes[bytesUsed:], nil
}
