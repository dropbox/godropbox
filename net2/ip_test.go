package net2

import (
	"strings"

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

func (s *IpSuite) TestMyHostnameAndIPs(c *C) {
	// Just make sure nothing crashes when calling the IP singletons.
	_, err := MyHostname()
	c.Assert(err, IsNil)
	_, err = MyIp4()
	c.Assert(err, IsNil)
	_, err = MyIp6()
	// It is ok to not have Ip6 address, but for now we should make sure we always have Ip4
	// addresses, since a lot of the code will not work otherwise.
	c.Assert(
		(err == nil) || (strings.Contains(err.Error(), "no suitable address found")),
		Equals,
		true)
}
