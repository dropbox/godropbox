package atomic2

import (
	"math"
	"sync/atomic"
)

// Float64 that supports atomic Get/Set/Add operations.
type AtomicFloat64 uint64

func (v *AtomicFloat64) Add(val float64) float64 {
	for {
		oldBits := atomic.LoadUint64((*uint64)(v))
		newVal := math.Float64frombits(oldBits) + val
		newBits := math.Float64bits(newVal)
		if atomic.CompareAndSwapUint64((*uint64)(v), oldBits, newBits) {
			return newVal
		}
	}
}

func (v *AtomicFloat64) Set(val float64) float64 {
	oldBits := atomic.SwapUint64((*uint64)(v), math.Float64bits(val))
	return math.Float64frombits(oldBits)
}

func (v *AtomicFloat64) Val() float64 {
	bits := atomic.LoadUint64((*uint64)(v))
	return math.Float64frombits(bits)
}
