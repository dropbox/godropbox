package memcache

import (
	"bytes"
	"context"

	. "gopkg.in/check.v1"

	. "godropbox/gocheck2"
)

type MockClientSuite struct {
	client *MockClient
}

var _ = Suite(&MockClientSuite{})

func (s *MockClientSuite) SetUpTest(c *C) {
	s.client = NewMockClient().(*MockClient)
}

func (s *MockClientSuite) TestAddSimple(c *C) {
	item := createTestItem()

	ctx := context.Background()
	resp := s.client.Add(ctx, item)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Key(), Equals, item.Key)
	c.Assert(resp.DataVersionId(), Equals, uint64(1))

	gresp := s.client.Get(ctx, item.Key)
	c.Assert(gresp.Error(), IsNil)
	c.Assert(bytes.Equal(gresp.Value(), item.Value), IsTrue)
	c.Assert(gresp.DataVersionId(), Equals, uint64(1))
}

func (s *MockClientSuite) TestAddExists(c *C) {
	item := createTestItem()

	ctx := context.Background()
	resp := s.client.Add(ctx, item)
	resp = s.client.Add(ctx, item)
	c.Assert(resp.Error(), Not(IsNil))
	c.Assert(resp.Status(), Equals, StatusItemNotStored)
}

func (s *MockClientSuite) TestAddMultiSimple(c *C) {
	item1 := createTestItem()
	item2 := createTestItem()
	item2.Key = "foo"
	items := []*Item{item1, item2}

	ctx := context.Background()
	resps := s.client.AddMulti(ctx, items)

	c.Assert(resps, HasLen, 2)
	for i := 0; i < 2; i++ {
		item := items[i]
		resp := resps[i]

		c.Assert(resp.Error(), IsNil)
		c.Assert(resp.Key(), Equals, item.Key)
		c.Assert(resp.DataVersionId(), Equals, uint64(1+i))

		gresp := s.client.Get(ctx, item.Key)
		c.Assert(gresp.Error(), IsNil)
		c.Assert(bytes.Equal(gresp.Value(), item.Value), IsTrue)
		c.Assert(gresp.DataVersionId(), Equals, uint64(1+i))
	}
}

func (s *MockClientSuite) TestAddMultiEmpty(c *C) {
	items := make([]*Item, 0)
	ctx := context.Background()
	resps := s.client.AddMulti(ctx, items)
	c.Assert(resps, HasLen, 0)
}

func (s *MockClientSuite) TestIncSimple(c *C) {
	// When incremening non existing item - initial value must be set, without actual increment
	resp := s.client.Increment(context.Background(), "test",
		5,  // inc delta
		10, // init value
		1)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Key(), Equals, "test")
	c.Assert(resp.Count(), Equals, uint64(10))

	// Now increment existing item
	resp = s.client.Increment(context.Background(), "test",
		5,  // inc delta
		10, // init value
		1)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Key(), Equals, "test")
	c.Assert(resp.Count(), Equals, uint64(15)) // 10 (init) + 5 (inc delta) = 15
}

func (s *MockClientSuite) TestDecSimple(c *C) {
	// When decremening non existing item - initial value must be set, without actual decreasement
	resp := s.client.Decrement(context.Background(), "test2",
		5,  // dec delta
		10, // init value
		1)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Key(), Equals, "test2")
	c.Assert(resp.Count(), Equals, uint64(10))

	// Now decrement existing item
	resp = s.client.Decrement(context.Background(), "test2",
		5,  // dec delta
		10, // init value
		1)
	c.Assert(resp.Error(), IsNil)
	c.Assert(resp.Key(), Equals, "test2")
	c.Assert(resp.Count(), Equals, uint64(5)) // 10 (init) - 5 (dec delta) = 5
}
