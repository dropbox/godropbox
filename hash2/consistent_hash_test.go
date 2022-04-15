package hash2

import "testing"

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
