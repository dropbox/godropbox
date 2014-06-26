package hash2

const (
	maxNumShards = 1<<16 - 1
)

// A simplified implementation of 32-bit murmur hash 3, which only accepts
// uint32 value as data, and uses 12345 as the seed.
//
// See https://code.google.com/p/smhasher/wiki/MurmurHash3 for details.
func simpleMurmur32(val uint32) uint32 {
	// body
	k := val * 0xcc9e2d51     // k = val * c1
	k = (k << 15) | (k >> 17) // k = rotl32(h, 15)
	k *= 0x1b873593           // k *= c2

	h := 12345 ^ k            // seed ^ k
	h = (h << 13) | (h >> 19) // k = rotl32(h, 13)
	h = h*5 + 0xe6546b64

	// finalize (NOTE: there's no tail)
	h = h ^ 4

	// fmix32
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}

// This implements a variant of consistent hashing. This implementation
// supports up to a maximum of (1 << 16 - 1) 65535 number of shards.
func ConsistentHash(key uint64, numShards uint16) uint16 {
	if numShards < 2 {
		return 0
	}

	hash := uint32(key) ^ uint32(key>>32)

	var lowestPositionShard uint16 = 0
	var minPosition uint16 = maxNumShards

	selectLowestPositionShard := func(shard, pos uint16) {
		pos %= (maxNumShards - shard)
		if pos < minPosition {
			lowestPositionShard = shard
			minPosition = pos
		}
	}

	numBlocks := numShards >> 1
	for i := uint16(0); i < numBlocks; i++ {
		// Each hash can generate 2 permutation positions
		hash = simpleMurmur32(hash)

		shard := i << 1

		selectLowestPositionShard(shard, uint16(hash))
		if minPosition == 0 {
			return lowestPositionShard
		}

		selectLowestPositionShard(shard+1, uint16(hash>>16))
		if minPosition == 0 {
			return lowestPositionShard
		}
	}

	if (numShards & 0x1) == 1 {
		hash = simpleMurmur32(hash)
		selectLowestPositionShard(numShards-1, uint16(hash))
	}

	return lowestPositionShard
}
