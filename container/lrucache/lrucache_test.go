package lrucache

import (
	"testing"

	"dropbox/util/testing2"
)

func TestLRUCache(t *testing.T) {
	h := testing2.H{t}
	c := New(2)
	c.Set("1", 1)
	c.Set("2", 2)
	c.Set("3", 9)
	h.AssertEquals(c.Len(), 2, "LRU cache length not correct")
	_, ok := c.Get("2")
	h.Assert(ok, "couldn't find key 2")
	_, ok = c.Get("3")
	h.Assert(ok, "couldn't find key 3")
	_, ok = c.Get("1")
	h.Assert(!ok, "key 1 should have been evicted")

	_, ok = c.Get("2")
	h.Assert(ok, "key 2 should still be in cache")
	c.Set("4", 4)
	_, ok = c.Get("1")
	h.Assert(!ok, "key 1 should have been evicted")
	_, ok = c.Get("3")
	h.Assert(!ok, "key 3 should have been evicted")
	_, ok = c.Get("2")
	h.Assert(ok, "couldn't find key 2")
	_, ok = c.Get("4")
	h.Assert(ok, "couldn't find key 4")
}
