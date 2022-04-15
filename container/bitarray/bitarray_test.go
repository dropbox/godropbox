package bitarray

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	insecure_rand "godropbox/math2/rand2"
)

var rand = insecure_rand.New(insecure_rand.NewSource(1))

// Helper to avoid direct calls to require in hot paths; in go 1.9 and later
// it becomes very expensive due to querying call stack context on every call.
func wrapRequire(t *testing.T, condition bool, msgAndArgs ...interface{}) {
	if !condition {
		require.Fail(t, "Fail", msgAndArgs...)
	}
}

func asize(n int) int {
	sz := n / 64
	if n%64 != 0 {
		sz++
	}
	return sz
}

func TestNew(t *testing.T) {
	a := New(-1)
	require.Nil(t, a)
	for n := 1000; n < 1064; n++ {
		a = New(n)
		wrapRequire(t, asize(n) == len(a.vs),
			fmt.Sprintf("n=%v, asize(n)=%v", n, asize(n)))
	}
}

func randArray(t *testing.T, n int) *BitArray {
	sz := asize(n)
	vs := make([]uint64, sz)
	for i := 0; i < sz; i++ {
		vs[i] = rand.Uint64()
	}
	a := NewUsingSlice(vs, n)
	wrapRequire(t, sz == len(a.vs))
	return a
}

var n = 1000

func TestIsSet(t *testing.T) {
	a := randArray(t, n)
	a.vs[3] = 298
	for i := 0; i < n; i++ {
		vsi := i / 64
		bitOff := uint32(i % 64)
		b := ((a.vs[vsi] >> bitOff) & 1) != 0
		wrapRequire(t, b == a.IsSet(i), "i=%v", i)
	}
}

func TestOrEquals(t *testing.T) {
	a1 := randArray(t, n)
	a2 := randArray(t, n)
	o := a1.Clone()
	o.OrEquals(a2)
	for i := 0; i < n; i++ {
		wrapRequire(t,
			o.IsSet(i) == (a1.IsSet(i) || a2.IsSet(i)),
			"i=%v", i)
	}
}

func TestSetClear(t *testing.T) {
	a := New(n)
	for j := 0; j < n; j++ {
		wrapRequire(t, !a.IsSet(j))
	}
	for i := 0; i < n; i++ {
		a.Set(i)
		for j := 0; j < n; j++ {
			wrapRequire(t, (i == j) == a.IsSet(j), "i=%v, j=%v", i, j)
		}
		a.Clear(i)
		for j := 0; j < n; j++ {
			wrapRequire(t, !a.IsSet(j), "i=%v, j=%v", i, j)
		}
	}
}

var ns = []int{
	62, 63, 64, 65, 66, 126, 127, 128, 129, 130, 131,
	190, 191, 192, 193, 194, 253, 254, 255, 256, 257, 258,
}

func TestIsAnySet(t *testing.T) {
	for _, n := range ns {
		a := New(n)
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				wrapRequire(t, !a.IsSet(j), "n=%v, i=%v, j=%v", n, i, j)
			}
			a.Set(i)
			wrapRequire(t, a.IsSet(i), "n=%v, i=%v", n, i)
			// parallel arrays.
			ss := []int{0, i - 1, i, i, i + 1, 0}
			es := []int{i - 1, i, i, i + 1, n - 1, n - 1}
			for j := 0; j < len(ss); j++ {
				s := ss[j]
				e := es[j]
				if s < 0 || e < 0 {
					continue
				}
				wrapRequire(t,
					(s <= i && i <= e) == a.IsAnySet(s, e),
					"n=%v, i=%v, s=%v, e=%v", n, i, s, e)
			}
			a.Clear(i)
		}
	}
}

func TestIsAnyClear(t *testing.T) {
	for _, n := range ns {
		a := New(n)
		a.SetRange(0, n-1)
		for i := 0; i < n; i++ {
			for j := 0; j < n; j++ {
				wrapRequire(t, a.IsSet(j), "n=%v, i=%v, j=%v", n, i, j)
			}
			a.Clear(i)
			wrapRequire(t, !a.IsSet(i), "n=%v, i=%v", n, i)
			// parallel arrays.
			ss := []int{0, i - 1, i, i, i + 1, 0}
			es := []int{i - 1, i, i, i + 1, n - 1, n - 1}
			for j := 0; j < len(ss); j++ {
				s := ss[j]
				e := es[j]
				if s < 0 || e < 0 {
					continue
				}
				wrapRequire(t,
					(s <= i && i <= e) == a.IsAnyClear(s, e),
					"n=%v, i=%v, s=%v, e=%v", n, i, s, e)
			}
			a.Set(i)
		}
	}
}

func TestSetRange(t *testing.T) {
	for _, n := range ns {
		for s := 0; s < n; s++ {
			for e := s; e < n; e++ {
				a := New(n)
				a.SetRange(s, e)
				msg := fmt.Sprintf("n=%v, s=%v, e=%v", n, s, e)
				if s > 0 {
					wrapRequire(t, !a.IsAnySet(0, s-1), "pre: "+msg)
				}
				for j := s; j <= e; j++ {
					wrapRequire(t, a.IsSet(j), "in: "+msg)
				}
				if e < n-1 {
					wrapRequire(t, !a.IsAnySet(e+1, n-1), "post: "+msg)
				}
			}
		}
	}
}

func TestClearRange(t *testing.T) {
	for _, n := range ns {
		for s := 0; s < n; s++ {
			for e := s; e < n; e++ {
				a := New(n)
				a.SetRange(0, n-1)
				a.ClearRange(s, e)
				msg := fmt.Sprintf("n=%v, s=%v, e=%v", n, s, e)
				if s > 0 {
					wrapRequire(t, !a.IsAnyClear(0, s-1), "pre: "+msg)
				}
				for j := s; j <= e; j++ {
					wrapRequire(t, !a.IsSet(j), "in: "+msg)
				}
				if e < n-1 {
					wrapRequire(t, !a.IsAnyClear(e+1, n-1), "post: "+msg)
				}
			}
		}
	}
}
