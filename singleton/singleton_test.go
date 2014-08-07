package singleton

import (
	"testing"

	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/errors"
)

func Test(t *testing.T) {
	TestingT(t)
}

type singletonSuite struct {
}

var _ = Suite(&singletonSuite{})

func (*singletonSuite) TestGet(c *C) {
	callCount := 0
	init := func() (interface{}, error) {
		callCount++

		if callCount < 2 {
			return nil, errors.Newf("fail")
		}
		v := 5
		return &v, nil
	}

	s := NewSingleton(init)
	// Verify init hasn't been called yet
	c.Assert(callCount, Equals, 0)

	// Verify init gets called with Get()
	_, err := s.Get()
	c.Assert(err, NotNil)
	c.Assert(callCount, Equals, 1)

	// Verify failed init gets retried with Get()
	data1, err := s.Get()
	c.Assert(err, IsNil)
	c.Assert(callCount, Equals, 2)
	c.Assert(*(data1.(*int)), Equals, 5)

	// Verify that subsequent Get() after a successful one return the same object and don't call
	// init
	data2, err := s.Get()
	c.Assert(err, IsNil)
	c.Assert(callCount, Equals, 2)
	c.Assert(data1, Equals, data2)
}
