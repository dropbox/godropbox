package caching

import (
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into go test runner
func Test(t *testing.T) {
	TestingT(t)
}

type GenericStorageSuite struct {
	local   Storage
	generic Storage
}

var _ = Suite(&GenericStorageSuite{})

func (s *GenericStorageSuite) SetUpTest(c *C) {
	// NOTE: GenericStorage using GetFunc, SetFunc, DelFunc, and FlushFunc are
	// tested via local map storage.
	s.local = NewLocalMapStorage("local", testKeyStr, testItemKeyStr)

	options := GenericStorageOptions{
		GetMultiFunc: s.local.GetMulti,
		SetMultiFunc: s.local.SetMulti,
		DelMultiFunc: s.local.DeleteMulti,
		ErrorOnFlush: true,
	}

	s.generic = NewGenericStorage("generic", options)
}

func (s *GenericStorageSuite) TestGet(c *C) {
	s.local.Set(&testKeyVal{"foo", 1})

	result, err := s.generic.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 1)
}

func (s *GenericStorageSuite) TestGetMulti(c *C) {
	s.local.SetMulti(&testKeyVal{"foo", 1}, &testKeyVal{"bar", 2})

	results, err := s.generic.GetMulti("foo", "zzz", "bar")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 3)

	c.Assert(results[0].(*testKeyVal).key, Equals, "foo")
	c.Assert(results[0].(*testKeyVal).val, Equals, 1)

	c.Assert(results[1], IsNil)

	c.Assert(results[2].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[2].(*testKeyVal).val, Equals, 2)
}

func (s *GenericStorageSuite) TestSet(c *C) {
	s.generic.Set(&testKeyVal{"foo", 1})

	result, err := s.generic.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 1)
}

func (s *GenericStorageSuite) TestSetMulti(c *C) {
	s.generic.SetMulti(&testKeyVal{"foo", 1}, &testKeyVal{"bar", 2})

	results, err := s.generic.GetMulti("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 2)

	c.Assert(results[0].(*testKeyVal).key, Equals, "foo")
	c.Assert(results[0].(*testKeyVal).val, Equals, 1)

	c.Assert(results[1].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[1].(*testKeyVal).val, Equals, 2)
}

func (s *GenericStorageSuite) TestDelete(c *C) {
	s.generic.SetMulti(&testKeyVal{"foo", 1}, &testKeyVal{"bar", 2})

	s.generic.Delete("foo")
	results, err := s.generic.GetMulti("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 2)

	c.Assert(results[0], IsNil)

	c.Assert(results[1].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[1].(*testKeyVal).val, Equals, 2)
}

func (s *GenericStorageSuite) TestDeleteMulti(c *C) {
	s.generic.SetMulti(
		&testKeyVal{"foo", 1},
		&testKeyVal{"bar", 2},
		&testKeyVal{"zzz", 3})

	s.generic.DeleteMulti("foo", "zzz")
	results, err := s.generic.GetMulti("foo", "bar", "zzz")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 3)

	c.Assert(results[0], IsNil)

	c.Assert(results[1].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[1].(*testKeyVal).val, Equals, 2)

	c.Assert(results[2], IsNil)
}

func (s *GenericStorageSuite) TestFlush(c *C) {
	s.generic.SetMulti(
		&testKeyVal{"foo", 1},
		&testKeyVal{"bar", 2},
		&testKeyVal{"zzz", 3})

	err := s.generic.Flush()
	c.Assert(err, NotNil)

	// Verify nothing got flushed.
	results, err := s.generic.GetMulti("foo", "bar", "zzz")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 3)

	c.Assert(results[0].(*testKeyVal).key, Equals, "foo")
	c.Assert(results[0].(*testKeyVal).val, Equals, 1)

	c.Assert(results[1].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[1].(*testKeyVal).val, Equals, 2)

	c.Assert(results[2].(*testKeyVal).key, Equals, "zzz")
	c.Assert(results[2].(*testKeyVal).val, Equals, 3)
}
