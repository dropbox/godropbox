package memcache

import (
	"log"

	"github.com/dropbox/godropbox/net2"
)

// A shard manager that returns connections from a static list of memcache
// shards.  NOTE: This is only for illustration purposes.  DO NOT USE IN
// PRODUCTION.
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
		shardStates[i].address = addr
		shardStates[i].state = ActiveServer
	}

	manager.UpdateShardStates(shardStates)

	return manager
}
