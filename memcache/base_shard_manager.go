package memcache

import (
	susanin_pb "dropbox/proto/susanin"
	"expvar"
	"fmt"
	"sync"
	"time"

	"godropbox/container/set"
	"godropbox/hash2/hashring"
	"godropbox/net2"
)

const (
	DefaultReadWriteTimeout = 50 * time.Millisecond
)

var (
	// Counters for number of connections that succeeded / errored / were skipped, by shard.
	connOkByShard  = expvar.NewMap("ShardManagerConnOkByShardCounter")
	connErrByShard = expvar.NewMap("ShardManagerConnErrByShardCounter")
)

func DefaultConnectionOptions() net2.ConnectionOptions {
	return net2.ConnectionOptions{
		MaxActiveConnections: -1, // unbounded,
		MaxIdleConnections:   16,
		// default Dial is net.DialTimeout(timeout=1s)
		DialMaxConcurrency: 10,
		ReadTimeout:        DefaultReadWriteTimeout,
		WriteTimeout:       DefaultReadWriteTimeout,
	}
}

// A base shard manager implementation that can be used to implement other
// shard managers.
type BaseShardManager struct {
	pool net2.ConnectionPool

	rwMutex       sync.RWMutex
	shards        []*susanin_pb.Address	// guarded by rwMutex
	hostToIp 	  map[string]string		// guarded by rwMutex
	shardHashRing *hashring.HashRing 	// guarded by rwMutex

	logError func(err error)
}

var _ ShardManager = (*BaseShardManager)(nil)

func NewBaseShardManagerWithOptions(
	logError func(err error),
	poolLogger func(pool net2.ConnectionPool),
	shards []*susanin_pb.Address,
	poolOptions net2.ConnectionOptions) *BaseShardManager {

	baseShardManger := &BaseShardManager{
		shardHashRing: hashring.New([]string{}),
		shards:        make([]*susanin_pb.Address, 0),
		hostToIp:	   make(map[string]string, 0),
		pool:          net2.NewMultiConnectionPool(poolOptions),
		logError:      logError,
	}
	baseShardManger.UpdateShards(shards)
	go poolLogger(baseShardManger.pool)

	return baseShardManger
}

func (m *BaseShardManager) UpdateShards(shards []*susanin_pb.Address) {
	newShards := set.NewSet()
	for _, shard := range shards {
		newShards.Add(*shard)
	}

	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	oldShards := set.NewSet()
	for _, shard := range m.shards {
		oldShards.Add(*shard)
	}

	shardHosts := make([]string, len(shards))
	for i, shard := range shards {
		shardHosts[i] = m.addrToHostPort(shard)
	}

	set.Subtract(newShards, oldShards).Do(func(shard interface{}) {
		addr := shard.(susanin_pb.Address)
		shardIpPort := m.addrToIpPort(&addr)
		if err := m.pool.Register("tcp", shardIpPort); err != nil {
			m.logError(err)
		}
		m.hostToIp[m.addrToHostPort(&addr)] = shardIpPort
	})

	set.Subtract(oldShards, newShards).Do(func(shard interface{}) {
		fmt.Printf("%v\n", shard)
		addr := shard.(susanin_pb.Address)
		shardIpPort := m.addrToIpPort(&addr)
		if err := m.pool.Unregister("tcp", shardIpPort); err != nil {
			m.logError(err)
		}
		delete(m.hostToIp, m.addrToHostPort(&addr))
	})

	m.shards = shards
	m.shardHashRing = hashring.New(shardHosts)
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetShard(
	key string) (
	conn net2.ManagedConn,
	err error) {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	shardIpPort, ok := m.hostToIp[m.shardHashRing.GetNode(key)]
	if !ok || shardIpPort == "" {
		err = noShardsError(key)
		return
	}

	entry := &ShardMapping{}
	m.fillEntryWithConnection(shardIpPort, entry)
	conn, err = entry.Connection, entry.ConnErr
	if err != nil {
		err = connectionError(shardIpPort, err)
	}

	return
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetShardsForKeys(
	keys []string) map[string]*ShardMapping {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	results := make(map[string]*ShardMapping)

	for _, key := range keys {
		shardIpPort, ok := m.hostToIp[m.shardHashRing.GetNode(key)]
		entry, inMap := results[shardIpPort]
		if !inMap {
			entry = &ShardMapping{}
			if ok && shardIpPort != "" {
				m.fillEntryWithConnection(shardIpPort, entry)
			}
			entry.Keys = make([]string, 0, 1)
			results[shardIpPort] = entry
		}
		entry.Keys = append(entry.Keys, key)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetShardsForItems(
	items []*Item) map[string]*ShardMapping {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	results := make(map[string]*ShardMapping)

	for _, item := range items {
		shardIpPort, ok := m.hostToIp[m.shardHashRing.GetNode(item.Key)]
		entry, inMap := results[shardIpPort]
		if !inMap {
			entry = &ShardMapping{}
			if ok && shardIpPort != "" {
				m.fillEntryWithConnection(shardIpPort, entry)
			}
			entry.Items = make([]*Item, 0, 1)
			entry.Keys = make([]string, 0, 1)
			results[shardIpPort] = entry
		}
		entry.Items = append(entry.Items, item)
		entry.Keys = append(entry.Keys, item.Key)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetAllShards() map[string]net2.ManagedConn {
	results := make(map[string]net2.ManagedConn)

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	for _, shard := range m.shards {
		shardIpPort := fmt.Sprintf("%s:%d", shard.Ip4, shard.Port)
		conn, err := m.pool.Get("tcp", shardIpPort)
		if err != nil {
			m.logError(err)
			conn = nil
		}
		results[shardIpPort] = conn
	}

	return results
}

func (m *BaseShardManager) fillEntryWithConnection(shardIpPort string, entry *ShardMapping) {
	conn, err := m.pool.Get("tcp", shardIpPort)
	if err != nil {
		m.logError(err)
		connErrByShard.Add(shardIpPort, 1)
		entry.ConnErr = err
	} else {
		connOkByShard.Add(shardIpPort, 1)
		entry.Connection = conn
	}
}

func (m *BaseShardManager) addrToIpPort(shard *susanin_pb.Address) string {
	return fmt.Sprintf("%s:%d", shard.Ip4, shard.Port)
}

func (m *BaseShardManager) addrToHostPort(shard *susanin_pb.Address) string {
	return fmt.Sprintf("%s:%d", shard.Host, shard.Port)
}
