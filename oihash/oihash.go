package oihash

import (
	"crypto/sha256"
	"encoding"
	"encoding/binary"

	"github.com/dropbox/godropbox/errors"
)

const (
	Size    = sha256.Size
	Columns = Size / 4
)

var primes = [Columns]uint32{4294967291, 4294967279, 4294967231, 4294967197, 4294967189, 4294967161, 4294967143, 4294967111}

type groupMem uint32
type OiHash struct {
	state [Columns]groupMem
}

// Interface assertions.
var _ encoding.BinaryMarshaler = &OiHash{}
var _ encoding.BinaryUnmarshaler = &OiHash{}

func addMod(i int, _a, _b groupMem) groupMem {
	a := uint64(_a)
	b := uint64(_b)
	c := (a + b) % uint64(primes[i])
	return groupMem(c)
}

func subMod(i int, _a, _b groupMem) groupMem {
	// use int64 because subtracting b from a might result in negative number
	a := int64(_a)
	b := int64(_b)
	c := (a - b) % int64(primes[i])

	// if the value is negative, add the value of mod operator
	if c < 0 {
		c = c + int64(primes[i])
	}
	// because primes[i] is a uint32, casting back to groupMem is safe
	return groupMem(c)
}

func mapInto(i int, x uint32) groupMem {
	v := uint64(x) % uint64(primes[i])
	return groupMem(v)
}

// Save an intermediate hash state for later.
func (h *OiHash) MarshalBinary() ([]byte, error) {
	var ret [Size]byte
	for column := range primes {
		offset := column * 4
		binary.LittleEndian.PutUint32(ret[offset:offset+4], uint32(h.state[column]))
	}
	return ret[:], nil
}

// Load a persisted hash state. Overwrites the internal state of h.
func (h *OiHash) UnmarshalBinary(data []byte) error {
	if len(data) != Size {
		return errors.Newf("can't unmarshal hash state: wrong size %d", len(data))
	}

	for column := range primes {
		offset := column * 4
		v := binary.LittleEndian.Uint32(data[offset : offset+4])
		if v >= primes[column] {
			return errors.Newf("can't decode hash: word %d: %d > max %d",
				column, v, primes[column])
		}
		h.state[column] = groupMem(v)
	}
	return nil
}

// Get the actual hash output.
//
// Happens to be the same as the MarshalBinary output, but named differently for API clarity.
func (h *OiHash) Digest() []byte {
	digest, err := h.MarshalBinary()
	// MarshalBinary doesn't generate any errors.
	if err != nil {
		panic("expected err to be nil, got " + err.Error())
	}

	return digest
}

func Hash(items [][]byte) OiHash {
	var ret OiHash
	for _, item := range items {
		ret.Insert(item)
	}
	return ret
}

func (h *OiHash) Insert(item []byte) {
	itemHash := sha256.Sum256(item)
	h.InsertHash(itemHash[:])
}

// Insert the sha256 sum of an item into the hash or a different marshalled oihash.
func (h *OiHash) InsertHash(hash []byte) {
	for i := 0; i < Columns; i++ {
		h.state[i] = addMod(i, h.state[i], mapInto(i, binary.LittleEndian.Uint32(hash[i*4:i*4+4])))
	}
}

func (h *OiHash) Remove(item []byte) {
	itemHash := sha256.Sum256(item)
	h.RemoveHash(itemHash[:])
}

// Remove the sha256 sum of an item from the hash
func (h *OiHash) RemoveHash(itemHash []byte) {
	for i := 0; i < Columns; i++ {
		h.state[i] = subMod(i, h.state[i], mapInto(i, binary.LittleEndian.Uint32(itemHash[i*4:i*4+4])))
	}
}

func Combine(lhs, rhs OiHash) OiHash {
	var ret OiHash
	for i := range lhs.state {
		ret.state[i] = addMod(i, lhs.state[i], rhs.state[i])
	}
	return ret
}

func NilHashToEmpty(h []byte) []byte {
	if h == nil {
		return (&OiHash{}).Digest()
	}
	return h
}
