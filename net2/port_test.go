package net2

import (
	"net"

	. "gopkg.in/check.v1"
)

type fakeAddr struct {
	network  string
	addrPort string
}

func (f *fakeAddr) Network() string {
	return f.network
}

func (f *fakeAddr) String() string {
	return f.addrPort
}

var _ net.Addr = &fakeAddr{}

type PortSuite struct {
}

var _ = Suite(&PortSuite{})

func (s *PortSuite) TestPort(c *C) {
	addr, err := GetPort(&fakeAddr{
		addrPort: "192.0.1.1:80",
	})
	c.Assert(err, IsNil)
	c.Assert(addr, Equals, 80)

	addr, err = GetPort(&fakeAddr{
		addrPort: "[::1]:80",
	})
	c.Assert(err, IsNil)
	c.Assert(addr, Equals, 80)

	addr, err = GetPort(&fakeAddr{
		addrPort: "192.0.1.1:0",
	})
	c.Assert(err, IsNil)
	c.Assert(addr, Equals, 0)

	addr, err = GetPort(&fakeAddr{
		addrPort: "[::1]:0",
	})
	c.Assert(err, IsNil)
	c.Assert(addr, Equals, 0)
}
