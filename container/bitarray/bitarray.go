package bitarray

import (
	"fmt"
)

type BitArray struct {
	vs []uint64
	n  int
}

func size(n int) int {
	sz := n >> 6
	if n&63 != 0 {
		sz++
	}
	return sz
}

func New(n int) *BitArray {
	if n < 0 {
		return nil
	}
	return &BitArray{
		vs: make([]uint64, size(n)),
		n:  n,
	}
}

func NewUsingSlice(vs []uint64, n int) *BitArray {
	return &BitArray{
		vs: vs[:size(n)],
		n:  n,
	}
}

func (b *BitArray) Clone() *BitArray {
	vs := make([]uint64, b.n)
	copy(vs, b.vs)
	return &BitArray{
		vs: vs,
		n:  b.n,
	}
}

func (b *BitArray) set(idx int, off uint32) {
	b.vs[idx] |= 1 << off
}

func (b *BitArray) clear(idx int, off uint32) {
	b.vs[idx] &= ^(1 << off)
}

func ioff(i int) (idx int, off uint32) {
	ui := uint32(i)
	idx = int(ui >> 6)
	off = ui & 63
	return
}

func (b *BitArray) Set(i int) {
	if i < 0 || i >= b.n {
		panic(fmt.Sprintf("index %v out of bounds [0:%v]", i, b.n))
	}
	idx, off := ioff(i)
	b.set(idx, off)
}

func (b *BitArray) Clear(i int) {
	if i < 0 || i >= b.n {
		panic(fmt.Sprintf("index %v out of bounds [0:%v]", i, b.n))
	}
	idx, off := ioff(i)
	b.clear(idx, off)
}

func (b *BitArray) isSet(idx int, off uint32) bool {
	return b.vs[idx]&(uint64(1)<<off) != 0
}

func (b *BitArray) IsSet(i int) bool {
	if i < 0 || i >= b.n {
		return false
	}
	idx, off := ioff(i)
	return b.isSet(idx, off)
}

// All the bits from off (inclusive) and above set.
func upperMask(off uint32) uint64 {
	return ^((uint64(1) << off) - 1)
}

// All the bits from off (inclusive) and below set.
func lowerMask(off uint32) uint64 {
	return (uint64(1) << (off + 1)) - 1
}

func (b *BitArray) IsAnySet(i0, i1 int) bool {
	return b.isAny(
		func(v uint64, mask uint64) bool { return (v & mask) != 0 },
		func(v uint64) bool { return v != 0 },
		i0,
		i1)
}

func (b *BitArray) IsAnyClear(i0, i1 int) bool {
	return b.isAny(
		func(v uint64, mask uint64) bool { return ((^v) & mask) != 0 },
		func(v uint64) bool { return v != ^uint64(0) },
		i0,
		i1)
}

func (b *BitArray) isAny(
	opBits func(v uint64, mask uint64) bool,
	opWord func(v uint64) bool,
	i0 int,
	i1 int,
) bool {
	idx0, off0 := ioff(i0)
	idx1, off1 := ioff(i1)
	if idx0 == idx1 {
		mask := upperMask(off0) & lowerMask(off1)
		return opBits(b.vs[idx0], mask)
	}
	if off0 != 0 {
		if opBits(b.vs[idx0], upperMask(off0)) {
			return true
		}
		idx0++
	}
	if off1 != 63 {
		if opBits(b.vs[idx1], lowerMask(off1)) {
			return true
		}
		idx1--
	}
	for idx := idx0; idx <= idx1; idx++ {
		if opWord(b.vs[idx]) {
			return true
		}
	}
	return false
}

func opRange(
	opBits func(idx int, off1 uint32, off2 uint32),
	opWord func(idx int),
	i0 int,
	i1 int,
) {
	idx0, off0 := ioff(i0)
	idx1, off1 := ioff(i1)
	if idx0 == idx1 {
		opBits(idx0, off0, off1)
		return
	}
	if off0 != 0 {
		opBits(idx0, off0, 63)
		idx0++
	}
	if off1 != 63 {
		opBits(idx1, 0, off1)
		idx1--
	}
	for idx := idx0; idx <= idx1; idx++ {
		opWord(idx)
	}
}

func (b *BitArray) setBits(idx int, off1 uint32, off2 uint32) {
	b.vs[idx] |= upperMask(off1) & lowerMask(off2)
}

func (b *BitArray) clearBits(idx int, off1 uint32, off2 uint32) {
	b.vs[idx] &= ^(upperMask(off1) & lowerMask(off2))
}

func (b *BitArray) SetRange(i0, i1 int) {
	opRange(
		b.setBits,
		func(idx int) { b.vs[idx] = ^uint64(0) },
		i0,
		i1)
}

func (b *BitArray) ClearRange(i0, i1 int) {
	opRange(
		b.clearBits,
		func(idx int) { b.vs[idx] = 0 },
		i0,
		i1)
}

func (b *BitArray) OrEquals(b2 *BitArray) {
	var n int
	if b.n <= b2.n {
		n = b.n
	} else {
		n = b2.n
	}
	endIdx := size(n) // Note the trailing zeroes don't matter.
	for idx := 0; idx < endIdx; idx++ {
		b.vs[idx] |= b2.vs[idx]
	}
}
