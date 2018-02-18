package hash2

import "testing"

func TestConsistentHash(t *testing.T) {
	c := ConsistentHash(uint64(0), 5)
	if c != 4 {
		t.Errorf("Invalid shard number returned")
	}

	c = ConsistentHash(uint64(0), 1)

	if c != 0 {
		t.Errorf("Shard number should be 0 when numShards is less than 3")
	}
}

func BenchmarkConsistentHashSmall(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ConsistentHash(uint64(i), 10)
	}
}

func BenchmarkConsistentHashMedium(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ConsistentHash(uint64(i), 100)
	}
}

func BenchmarkConsistentHashLarge(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ConsistentHash(uint64(i), 1000)
	}
}

func BenchmarkConsistentHashExtraLarge(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ConsistentHash(uint64(i), 10000)
	}
}
