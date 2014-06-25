package caching

import (
	. "gopkg.in/check.v1"
)

type CacheOnStorageSuite struct {
	cache    Storage
	storage  Storage
	combined Storage
}

var _ = Suite(&CacheOnStorageSuite{})

func (s *CacheOnStorageSuite) SetUpTest(c *C) {
	s.cache = NewLocalMapStorage("cache", testKeyStr, testItemKeyStr)
	s.storage = NewLocalMapStorage("storage", testKeyStr, testItemKeyStr)
	s.combined = NewCacheOnStorage(s.cache, s.storage)
}

func (s *CacheOnStorageSuite) TestGetCacheHit(c *C) {
	s.cache.Set(&testKeyVal{"foo", 1})
	s.storage.Set(&testKeyVal{"foo", 10})

	result, err := s.combined.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 1)
}

func (s *CacheOnStorageSuite) TestGetCacheMiss(c *C) {
	s.storage.Set(&testKeyVal{"foo", 10})

	result, err := s.combined.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 10)

	// cache set
	result, err = s.cache.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 10)
}

func (s *CacheOnStorageSuite) TestGetMulti(c *C) {
	s.cache.Set(&testKeyVal{"zzz", 1})
	s.storage.Set(&testKeyVal{"foo", 10})
	s.storage.Set(&testKeyVal{"zzz", 123})

	results, err := s.combined.GetMulti("foo", "bar", "zzz")
	c.Assert(err, IsNil)
	c.Assert(len(results), Equals, 3)

	// Cache missed
	c.Assert(results[0].(*testKeyVal).key, Equals, "foo")
	c.Assert(results[0].(*testKeyVal).val, Equals, 10)

	// Not in either storage
	c.Assert(results[1], IsNil)

	// Cache hit
	c.Assert(results[2].(*testKeyVal).key, Equals, "zzz")
	c.Assert(results[2].(*testKeyVal).val, Equals, 1)

	// foo inserted into cache
	result, err := s.cache.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 10)
}

func (s *CacheOnStorageSuite) TestSet(c *C) {
	s.combined.Set(&testKeyVal{"foo", 123})

	// set in cache
	result, err := s.cache.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 123)

	// set in storage
	result, err = s.storage.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 123)
}

func (s *CacheOnStorageSuite) TestSetMulti(c *C) {
	s.combined.SetMulti(
		&testKeyVal{"foo", 123},
		&testKeyVal{"bar", 321})

	// set in s.cache
	result, err := s.cache.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 123)

	result, err = s.cache.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "bar")
	c.Assert(result.(*testKeyVal).val, Equals, 321)

	// set in s.storage
	result, err = s.storage.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "foo")
	c.Assert(result.(*testKeyVal).val, Equals, 123)

	result, err = s.storage.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "bar")
	c.Assert(result.(*testKeyVal).val, Equals, 321)
}

func (s *CacheOnStorageSuite) TestDelete(c *C) {
	s.combined.SetMulti(
		&testKeyVal{"foo", 123},
		&testKeyVal{"bar", 321})

	s.combined.Delete("foo")

	// set in s.cache
	result, err := s.cache.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.cache.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "bar")
	c.Assert(result.(*testKeyVal).val, Equals, 321)

	// set in s.storage
	result, err = s.storage.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.storage.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "bar")
	c.Assert(result.(*testKeyVal).val, Equals, 321)
}

func (s *CacheOnStorageSuite) TestDeleteMulti(c *C) {
	s.combined.SetMulti(
		&testKeyVal{"foo", 123},
		&testKeyVal{"bar", 321},
		&testKeyVal{"zzz", 213})

	s.combined.DeleteMulti("foo", "zzz")

	// set in s.cache
	result, err := s.cache.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.cache.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "bar")
	c.Assert(result.(*testKeyVal).val, Equals, 321)

	result, err = s.cache.Get("zzz")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	// set in s.storage
	result, err = s.storage.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.storage.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.(*testKeyVal).key, Equals, "bar")
	c.Assert(result.(*testKeyVal).val, Equals, 321)

	result, err = s.storage.Get("zzz")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)
}

func (s *CacheOnStorageSuite) TestFlush(c *C) {
	s.combined.SetMulti(
		&testKeyVal{"foo", 123},
		&testKeyVal{"bar", 321},
		&testKeyVal{"zzz", 213})

	s.combined.Flush()

	// set in s.cache
	result, err := s.cache.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.cache.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.cache.Get("zzz")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	// set in s.storage
	result, err = s.storage.Get("foo")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.storage.Get("bar")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)

	result, err = s.storage.Get("zzz")
	c.Assert(err, IsNil)
	c.Assert(result, IsNil)
}
