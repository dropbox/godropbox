package murmur3

import "encoding/binary"

const (
	c1_32 uint32 = 0xcc9e2d51
	c2_32 uint32 = 0x1b873593
)

// murmur3 32bit hash function implementation,
// github.com/spaolacci/murmur3 used as reference, but removed uses of unsafe.
func Hash32(data []byte, seed uint32) uint32 {
	h1 := seed

	nblocks := len(data) / 4
	for idx := 0; idx < nblocks; idx++ {
		k1 := binary.LittleEndian.Uint32(data[idx*4 : idx*4+4])

		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19) // rotl32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	tail := data[nblocks*4:]

	var k1 uint32
	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1_32
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2_32
		h1 ^= k1
	}

	h1 ^= uint32(len(data))

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16
	return h1
}
