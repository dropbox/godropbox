package sync2

import (
	"sync/atomic"
	"time"
)

type AtomicInt32 int32

func (i32 *AtomicInt32) Add(n int32) int32 {
	return atomic.AddInt32((*int32)(i32), n)
}

func (i32 *AtomicInt32) Set(n int32) {
	atomic.StoreInt32((*int32)(i32), n)
}

func (i32 *AtomicInt32) Get() int32 {
	return atomic.LoadInt32((*int32)(i32))
}

func (i32 *AtomicInt32) CompareAndSwap(oldval, newval int32) (swapped bool) {
	return atomic.CompareAndSwapInt32((*int32)(i32), oldval, newval)
}

type AtomicUint32 uint32

func (u32 *AtomicUint32) Add(n uint32) uint32 {
	return atomic.AddUint32((*uint32)(u32), n)
}

func (u32 *AtomicUint32) Set(n uint32) {
	atomic.StoreUint32((*uint32)(u32), n)
}

func (u32 *AtomicUint32) Get() uint32 {
	return atomic.LoadUint32((*uint32)(u32))
}

func (u32 *AtomicUint32) CompareAndSwap(oldval, newval uint32) (swapped bool) {
	return atomic.CompareAndSwapUint32((*uint32)(u32), oldval, newval)
}

type AtomicInt64 int64

func (i64 *AtomicInt64) Add(n int64) int64 {
	return atomic.AddInt64((*int64)(i64), n)
}

func (i64 *AtomicInt64) Set(n int64) {
	atomic.StoreInt64((*int64)(i64), n)
}

func (i64 *AtomicInt64) Get() int64 {
	return atomic.LoadInt64((*int64)(i64))
}

func (i64 *AtomicInt64) CompareAndSwap(oldval, newval int64) (swapped bool) {
	return atomic.CompareAndSwapInt64((*int64)(i64), oldval, newval)
}

type AtomicDuration int64

func (dur *AtomicDuration) Add(dururation time.Duration) time.Duration {
	return time.Duration(atomic.AddInt64((*int64)(dur), int64(dururation)))
}

func (dur *AtomicDuration) Set(dururation time.Duration) {
	atomic.StoreInt64((*int64)(dur), int64(dururation))
}

func (dur *AtomicDuration) Get() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(dur)))
}

func (dur *AtomicDuration) CompareAndSwap(oldval, newval time.Duration) (swapped bool) {
	return atomic.CompareAndSwapInt64((*int64)(dur), int64(oldval), int64(newval))
}
