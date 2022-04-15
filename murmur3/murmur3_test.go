package murmur3

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type Murmur3Suite struct{}

var _ = Suite(&Murmur3Suite{})

var (
	TEST_SEED = 0x123456
)

func (s *Murmur3Suite) TestHash32(c *C) {
	kvals := map[string]uint32{
		"":                uint32(0x6e9c81dc),
		"1":               uint32(0x9c5063b),
		"12":              uint32(0xff0b3b72),
		"123":             uint32(0xd44872db),
		"1234":            uint32(0x6e5901c4),
		"test1":           uint32(0xa09f7757),
		"foobar":          uint32(0xaea81970),
		"testtest":        uint32(0x333824c2),
		"testtest2":       uint32(0x8aae8a95),
		"foobarfoobar":    uint32(0xd3d9161a),
		"foobarfoobarfoo": uint32(0x408db283),
	}
	for key, val := range kvals {
		hash := Hash32([]byte(key), uint32(TEST_SEED))
		c.Assert(hash, Equals, val)
	}
}
