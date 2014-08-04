package net2

import (
	. "gopkg.in/check.v1"

	. "github.com/dropbox/godropbox/gocheck2"
)

type IpSuite struct {
}

var _ = Suite(&IpSuite{})

func (s *IpSuite) TestIsLocalhost(c *C) {
	c.Assert(IsLocalhost("localhost"), IsTrue)
	c.Assert(IsLocalhost("ip6-localhost"), IsTrue)
	c.Assert(IsLocalhost("ipv6-localhost"), IsTrue)
	c.Assert(IsLocalhost("dropbox.com"), IsFalse)
	c.Assert(IsLocalhost("google.com"), IsFalse)
}
