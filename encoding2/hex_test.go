package encoding2

import (
	"bytes"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type HexSuite struct {
}

var _ = Suite(&HexSuite{})

func (s *HexSuite) TestBasicStreamHex(c *C) {
	w := bytes.NewBuffer(nil)
	HexEncodeToWriter(w, []byte("foo"))
	c.Assert(w.String(), Equals, "666f6f")

	w = bytes.NewBuffer(nil)
	HexEncodeToWriter(w, []byte(""))
	c.Assert(w.String(), Equals, "")

	w = bytes.NewBuffer(nil)
	HexEncodeToWriter(w, []byte("\x00\x01"))
	c.Assert(w.String(), Equals, "0001")
}
