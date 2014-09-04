package memcache

import (
	. "gopkg.in/check.v1"

	"github.com/dropbox/godropbox/container/set"
	. "github.com/dropbox/godropbox/gocheck2"
	"github.com/dropbox/godropbox/net2"
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

func (p *mockPool) Register(_, address string) error {
	p.registered.Add(address)
	return nil
}

func (p *mockPool) Unregister(_, address string) error {
	p.registered.Remove(address)
	return nil
}

func (s *ManagerSuite) SetUpTest(c *C) {
	s.pool = newMockPool()
	s.manager = &BaseShardManager{
		pool: s.pool,
	}
}

func (s *ManagerSuite) TestRegister(c *C) {
	shardStates := make([]ShardState, 4)
	shardStates[0].Address = "foo"
	shardStates[1].Address = "bar"
	shardStates[2].Address = "baz"
	shardStates[3].Address = "foo"
	s.manager.UpdateShardStates(shardStates)

	expectedRegistered := set.NewSet("foo", "bar", "baz")
	c.Assert(expectedRegistered.IsEqual(s.pool.registered), IsTrue)
}

func (s *ManagerSuite) TestUnregister(c *C) {
	shardStates := make([]ShardState, 4)
	shardStates[0].Address = "foo"
	shardStates[1].Address = "bar"
	shardStates[2].Address = "baz"
	shardStates[3].Address = "foo"
	s.manager.UpdateShardStates(shardStates)

	shardStates = make([]ShardState, 2)
	shardStates[0].Address = "bar"
	shardStates[1].Address = "baz"
	s.manager.UpdateShardStates(shardStates)
	expectedRegistered := set.NewSet("bar", "baz")
	c.Assert(expectedRegistered.IsEqual(s.pool.registered), IsTrue)
}
