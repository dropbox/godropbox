package oihash

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	r := require.New(t)

	h := Hash([][]byte{[]byte("hello")})
	r.EqualValues(
		OiHash{[Columns]groupMem{0xba4df22c, 0xea3b05f, 0x2a3be826, 0x9ee2b9c5, 0x5c1e161b, 0x5e42a71f, 0x62330473, 0x24988b93}},
		h)

	r.EqualValues(
		[]byte{
			0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0xe, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9, 0xe2, 0x9e,
			0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x4, 0x33, 0x62, 0x93, 0x8b, 0x98, 0x24},
		h.Digest())
}

func TestEmpty(t *testing.T) {
	r := require.New(t)

	h := Hash([][]byte{})
	r.EqualValues(
		OiHash{[Columns]groupMem{0, 0, 0, 0, 0, 0, 0, 0}},
		h)

	r.EqualValues(
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		h.Digest())
}

func TestCommutative(t *testing.T) {
	r := require.New(t)

	a := []byte("a")
	b := []byte("b")

	r.EqualValues(Hash([][]byte{a, b}), Hash([][]byte{b, a}))
}

func TestCombine(t *testing.T) {
	r := require.New(t)

	a := []byte("a")
	b := []byte("b")

	ha := Hash([][]byte{a})
	hb := Hash([][]byte{b})
	hUnion := Hash([][]byte{a, b})

	c := Combine(ha, hb)

	r.EqualValues(hUnion, c)
}

func TestCombineOneEmpty(t *testing.T) {
	r := require.New(t)

	ha := Hash([][]byte{[]byte("a")})
	hb := Hash([][]byte{})

	hc := Combine(ha, hb)

	r.EqualValues(ha, hc)
}

func TestInsertAndRemove(t *testing.T) {
	r := require.New(t)

	a := []byte("a")
	b := []byte("b")
	c := []byte("c")
	d := []byte("d")
	union := [][]byte{a, b, c, d}

	hUnion := Hash(union)

	hashes := make(map[OiHash]struct{})
	var h OiHash
	hashes[h] = struct{}{}

	for _, item := range union {
		h.Insert(item)

		if _, ok := hashes[h]; ok {
			t.Fatalf("duplicate hash %x", h)
		}

		hashes[h] = struct{}{}
	}

	r.Equal(hUnion, h)
	r.Len(hashes, 5)

	// Remove two of the items we added
	toRemove := [][]byte{c, d}
	for _, item := range toRemove {
		h.Remove(item)
	}
	// Now after removing the hash of {c, d} from the hash of {a,b,c,d}, we should be left with just the hash of {a,b}
	remaining := [][]byte{a, b}
	hExpected := Hash(remaining)
	r.Equal(hExpected, h)
}

func TestInsertHashIsSameAsCombine(t *testing.T) {
	a := &OiHash{}
	b := &OiHash{}
	a.Insert([]byte("a"))
	b.Insert([]byte("b"))

	combined := Combine(*a, *b)
	a.InsertHash(b.Digest())
	require.Equal(t, combined.Digest(), a.Digest())
}

func TestRemoveHash(t *testing.T) {
	r := require.New(t)

	a := []byte("a")
	b := []byte("b")
	c := []byte("c")
	d := []byte("d")
	h1 := Hash([][]byte{a, b, c, d})
	h2 := Hash([][]byte{c, d})

	h1.RemoveHash(h2.Digest())

	r.Equal(Hash([][]byte{a, b}), h1)
}

func assertDistinctHashes(t *testing.T, multiSets ...[]string) {
	hashSet := make(map[OiHash][]string)
	for _, multiSet := range multiSets {
		var mset [][]byte
		for _, s := range multiSet {
			mset = append(mset, []byte(s))
		}

		h := Hash(mset)

		if _, ok := hashSet[h]; ok {
			t.Fatalf("%v collides with %v with hash %x",
				multiSet, hashSet[h], h)
		}

		hashSet[h] = multiSet
	}
}

func TestDistinctSimple(t *testing.T) {
	a := "a"
	b := "b"

	assertDistinctHashes(t, []string{a}, []string{b})
	assertDistinctHashes(t, []string{a}, []string{a, a})
	assertDistinctHashes(t, []string{a, b}, []string{a, b, b})
}

func TestDistinctDuplicateCounts(t *testing.T) {
	r := require.New(t)

	const numDupes = 10000
	a := []byte("a")

	hashes := make(map[OiHash]struct{})
	var h OiHash
	hashes[h] = struct{}{}

	for i := 0; i < numDupes; i++ {
		h.Insert(a)

		if _, ok := hashes[h]; ok {
			t.Fatalf("duplicate hash %x", h)
		}

		hashes[h] = struct{}{}
	}

	r.Len(hashes, numDupes+1)
}

func TestMarshaling(t *testing.T) {
	r := require.New(t)

	a := []byte("a")

	h := Hash([][]byte{a})
	ser, err := h.MarshalBinary()
	r.NoError(err)
	r.Len(ser, Size)

	var deser OiHash
	err = deser.UnmarshalBinary(ser)
	r.NoError(err)

	r.Equal(h, deser)
}

func BenchmarkInserts(b *testing.B) {
	item := make([]byte, 1000)
	_, err := rand.Read(item)
	if err != nil {
		b.Fatal(err)
	}

	var h OiHash
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Insert(item)
	}
}
