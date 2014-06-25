package caching

import (
	. "gopkg.in/check.v1"
)

type testKeyVal struct {
	key string
	val int
}

func testKeyStr(key interface{}) string {
	return key.(string)
}

func testItemKeyStr(item interface{}) string {
	return item.(*testKeyVal).key
}

type LocalMapStorageSuite struct {
	localMap *localMapStorage
	storage  Storage
}

var _ = Suite(&LocalMapStorageSuite{})

func (s *LocalMapStorageSuite) SetUpTest(c *C) {
	local, store := newLocalMapStorage("test", testKeyStr, testItemKeyStr)
	s.localMap = local
	s.storage = store
}

func (s *LocalMapStorageSuite) TestGet(c *C) {
	s.localMap.set(&testKeyVal{"foo", 1})

	result, err := s.storage.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 1)
}

func (s *LocalMapStorageSuite) TestGetMulti(c *C) {
	s.localMap.set(&testKeyVal{"foo", 1})
	s.localMap.set(&testKeyVal{"bar", 2})

	results, err := s.storage.GetMulti("foo", "zzz", "bar")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 3)

	c.Assert(results[0].(*testKeyVal).key, Equals, "foo")
	c.Assert(results[0].(*testKeyVal).val, Equals, 1)

	c.Assert(results[1], IsNil)

	c.Assert(results[2].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[2].(*testKeyVal).val, Equals, 2)
}

func (s *LocalMapStorageSuite) TestSet(c *C) {
	s.storage.Set(&testKeyVal{"foo", 1})

	result, err := s.storage.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 1)
}

func (s *LocalMapStorageSuite) TestSetMulti(c *C) {
	s.storage.SetMulti(&testKeyVal{"foo", 1}, &testKeyVal{"bar", 2})

	results, err := s.storage.GetMulti("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 2)

	c.Assert(results[0].(*testKeyVal).key, Equals, "foo")
	c.Assert(results[0].(*testKeyVal).val, Equals, 1)

	c.Assert(results[1].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[1].(*testKeyVal).val, Equals, 2)
}

func (s *LocalMapStorageSuite) TestDelete(c *C) {
	s.storage.SetMulti(&testKeyVal{"foo", 1}, &testKeyVal{"bar", 2})

	s.storage.Delete("foo")
	results, err := s.storage.GetMulti("foo", "bar")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 2)

	c.Assert(results[0], IsNil)

	c.Assert(results[1].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[1].(*testKeyVal).val, Equals, 2)
}

func (s *LocalMapStorageSuite) TestDeleteMulti(c *C) {
	s.storage.SetMulti(
		&testKeyVal{"foo", 1},
		&testKeyVal{"bar", 2},
		&testKeyVal{"zzz", 3})

	s.storage.DeleteMulti("foo", "zzz")
	results, err := s.storage.GetMulti("foo", "bar", "zzz")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 3)

	c.Assert(results[0], IsNil)

	c.Assert(results[1].(*testKeyVal).key, Equals, "bar")
	c.Assert(results[1].(*testKeyVal).val, Equals, 2)

	c.Assert(results[2], IsNil)
}

func (s *LocalMapStorageSuite) TestFlush(c *C) {
	s.storage.SetMulti(
		&testKeyVal{"foo", 1},
		&testKeyVal{"bar", 2},
		&testKeyVal{"zzz", 3})

	err := s.storage.Flush()
	c.Assert(err, IsNil)
	c.Assert(s.localMap.size(), Equals, 0)
}
