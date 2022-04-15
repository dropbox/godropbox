package time2

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestFloatTimeConversion(t *testing.T) {
	// Some specific date.
	t1 := time.Unix(1147483647, 0)
	f := TimeToFloat(t1)
	t2 := FloatToTime(f)
	assert.Equal(t, t1, t2)
}

func TestArbitrary(t *testing.T) {
	assert.Equal(t, Arbitrary(0), Arbitrary(0))
	assert.Equal(t, Arbitrary(-1934), Arbitrary(-1934))
	// These are allowed to be equal, but aren't expected to be.
	// This just validates that we aren't always returning the same date.
	assert.NotEqual(t, Arbitrary(4), Arbitrary(10000))
}

// As of late 2018, the results of the following 2 microbenchmarks were:
//
//   $ grep model\ name  /proc/cpuinfo | uniq
//   model name	: Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
//   $ ./time2_test_1.10_bin --test.bench=.
//   OK: 1 passed
//   Test finished.
//   goos: linux
//   goarch: amd64
//   BenchmarkTimeNow-40      	20000000	        82.7 ns/op
//   BenchmarkMonoClock-40    	 1000000	      1032 ns/op
//
// i.e., the standard library was ~12x faster than MonoClock, despite the fact
// that time.Now is also monotonic.  The reason for this is the standard
// library uses vDSO to read the system timestamp counter from a magic page of
// memory which is not really a syscall.  MonoClock uses syscall.Syscall
// directly, bypassing vDSO.  vDSO implementation is very important for
// frequent syscalls like gettimeofday, clock_gettime, and getcpu.
//
// On the other hand, the direct syscall implementation of MonoClock is faster
// on AWS, because vDSO does not work on AWS and time.Now falls back to
// syscalls.  time.Now reads both CLOCK_MONOTONIC and CLOCK_REALTIME, so it is
// slower.
//
// After MonoClock is converted to use time.Now, these microbenchmarks will
// have the same results.
//
// See https://github.com/golang/proposal/blob/master/design/12914-monotonic.md

func BenchmarkTimeNow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now()
	}
}

func BenchmarkMonoClock(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MonoClock()
	}
}
