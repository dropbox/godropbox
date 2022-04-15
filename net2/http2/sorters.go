package http2

import (
	"crypto/md5"
	"encoding/binary"
)

type instancePoolSlice []*instancePool

func (s instancePoolSlice) Len() int      { return len(s) }
func (s instancePoolSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// instancePoolSlice sorts by instanceId in descending order.
func (s instancePoolSlice) Less(i, j int) bool { return s[i].instanceId > s[j].instanceId }

type shuffleSortHelper struct {
	shuffleSeed int
	instances   []*instancePool
}

func (s shuffleSortHelper) sortIdx(idx int) uint64 {
	addr := s.instances[idx].addr
	buffer := make([]byte, 8+len(addr))
	binary.LittleEndian.PutUint64(buffer[:8], uint64(s.shuffleSeed))
	copy(buffer[8:], addr)
	sum := md5.Sum(buffer)
	return binary.LittleEndian.Uint64(sum[:8])
}
func (s shuffleSortHelper) Len() int { return len(s.instances) }
func (s shuffleSortHelper) Swap(i, j int) {
	s.instances[i], s.instances[j] = s.instances[j], s.instances[i]
}
func (s shuffleSortHelper) Less(i, j int) bool { return s.sortIdx(i) < s.sortIdx(j) }

// Consistent hashing
type consistentHashSortHelper struct {
	Instances []*instancePool
	Hashes    []uint32
}

func (s consistentHashSortHelper) Len() int { return len(s.Instances) }
func (s consistentHashSortHelper) Swap(i, j int) {
	s.Instances[i], s.Instances[j] = s.Instances[j], s.Instances[i]
	s.Hashes[i], s.Hashes[j] = s.Hashes[j], s.Hashes[i]
}
func (s consistentHashSortHelper) Less(i, j int) bool { return s.Hashes[i] < s.Hashes[j] }

// Recency sort
type recencySortHelper struct {
	Instances []*instancePool
}

func (s recencySortHelper) Len() int { return len(s.Instances) }
func (s recencySortHelper) Swap(i, j int) {
	s.Instances[i], s.Instances[j] = s.Instances[j], s.Instances[i]
}
func (s recencySortHelper) Less(i, j int) bool {
	// Sort in REVERSE order of added time
	return s.Instances[i].addedTimeNano >= s.Instances[j].addedTimeNano
}
