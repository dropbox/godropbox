package memcache

import (
	susanin_pb "dropbox/proto/susanin"
	. "gopkg.in/check.v1"

	"godropbox/container/set"
	. "godropbox/gocheck2"
	"godropbox/net2"
)

type ManagerSuite struct {
	manager *BaseShardManager
	pool    *mockPool
}

var _ = Suite(&ManagerSuite{})

type mockPool struct {
	net2.ConnectionPool
	registered set.Set
}

func newMockPool() *mockPool {
	return &mockPool{
		registered: set.NewSet(),
	}
}

func (p *mockPool) Register(_, shardIpPort string) error {
	p.registered.Add(shardIpPort)
	return nil
}

func (p *mockPool) Unregister(_, shardIpPort string) error {
	p.registered.Remove(shardIpPort)
	return nil
}

func (s *ManagerSuite) SetUpTest(c *C) {
	s.pool = newMockPool()
	s.manager = &BaseShardManager{
		pool:     s.pool,
		hostToIp: map[string]string{},
	}
}

func (s *ManagerSuite) TestRegisterAndDeregister(c *C) {
	shards := []*susanin_pb.Address {
		{
			Host: "foo",
			Ip4:  "fooIp",
			Port: 5211,
		},
		{
			Host: "bar",
			Ip4:  "barIp",
			Port: 5211,
		},
		{
			Host: "baz",
			Ip4:  "bazIp",
			Port: 5211,
		},
		{
			Host: "foo",
			Ip4:  "fooIp",
			Port: 5211,
		},
	}
	s.manager.UpdateShards(shards)

	expectedRegistered := set.NewSet("fooIp:5211", "barIp:5211", "bazIp:5211")
	c.Assert(expectedRegistered.IsEqual(s.pool.registered), IsTrue)

	shards = []*susanin_pb.Address {
		{
			Host: "bar",
			Ip4:  "barIp",
			Port: 5211,
		},
		{
			Host: "baz",
			Ip4:  "bazIp",
			Port: 5211,
		},
	}
	s.manager.UpdateShards(shards)
	expectedRegistered = set.NewSet("barIp:5211", "bazIp:5211")
	c.Assert(expectedRegistered.IsEqual(s.pool.registered), IsTrue)

	shards = []*susanin_pb.Address{}
	s.manager.UpdateShards(shards)
	expectedRegistered = set.NewSet()
	c.Assert(expectedRegistered.IsEqual(s.pool.registered), IsTrue)
}
