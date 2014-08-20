package memcache

import (
	"log"

	"github.com/dropbox/godropbox/net2"
)

// A shard manager that returns connections from a static list of memcache
// shards.  NOTE: This is only for illustration purposes.  DO NOT USE IN
// PRODUCTION.  (Dropbox internally uses a different shard manager which is
// also based on BaseShardManager.  Our memcache config is managed by zookeeper.
// When our memcache config changes, zookeeper will notify the shard manager of
// these updates and the shard manager will in turn swap in/out shards via
// UpdateShardStates.)
type StaticShardManager struct {
	BaseShardManager
}

// This creates a StaticShardManager, which returns connections from a static
// list of memcache shards.
func NewStaticShardManager(
	serverAddrs []string,
	shardFunc func(key string, numShard int) (shard int),
	options net2.ConnectionOptions) ShardManager {

	manager := &StaticShardManager{}
	manager.Init(
		shardFunc,
		func(err error) { log.Print(err) },
		log.Print,
		options)

	shardStates := make([]ShardState, len(serverAddrs), len(serverAddrs))
	for i, addr := range serverAddrs {
		shardStates[i].Address = addr
		shardStates[i].State = ActiveServer
	}

	manager.UpdateShardStates(shardStates)

	return manager
}
