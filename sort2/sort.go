package sort2

import (
	"sort"
)

// Implememtation of some useful slice sorts
type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint64Slice) Sort() {
	sort.Sort(s)
}

func Uint64s(s []uint64) {
	sort.Sort(Uint64Slice(s))
}
