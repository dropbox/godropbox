package memcache

import (
	"expvar"
	"sync"

	"github.com/dropbox/godropbox/container/set"
	"github.com/dropbox/godropbox/net2"
)

type MemcachedState int

const (
	ActiveServer    = MemcachedState(0)
	WriteOnlyServer = MemcachedState(1)
	DownServer      = MemcachedState(2)
	WarmUpServer    = MemcachedState(4)
)

var (
	// Counters for number of connections that succeeded / errored / were skipped, by address.
	connOkByAddr      = expvar.NewMap("ShardManagerConnOkByAddrCounter")
	connErrByAddr     = expvar.NewMap("ShardManagerConnErrByAddrCounter")
	connSkippedByAddr = expvar.NewMap("ShardManagerConnSkippedByAddrCounter")
)

type ShardState struct {
	Address string
	State   MemcachedState
}

// A base shard manager implementation that can be used to implement other
// shard managers.
type BaseShardManager struct {
	getShardId (func(key string, numShard int) (shard int))
	pool       net2.ConnectionPool

	rwMutex     sync.RWMutex
	shardStates []ShardState // guarded by rwMutex

	logError func(err error)
	logInfo  func(v ...interface{})
}

// Initializes the BaseShardManager.
func (m *BaseShardManager) Init(
	shardFunc func(key string, numShard int) (shard int),
	logError func(err error),
	logInfo func(v ...interface{}),
	options net2.ConnectionOptions) {

	m.InitWithPool(
		shardFunc,
		logError,
		logInfo,
		net2.NewMultiConnectionPool(options))
}

func (m *BaseShardManager) InitWithPool(
	shardFunc func(key string, numShard int) (shard int),
	logError func(err error),
	logInfo func(v ...interface{}),
	pool net2.ConnectionPool) {

	m.shardStates = make([]ShardState, 0, 0)
	m.getShardId = shardFunc
	m.pool = pool

	m.logError = logError
	m.logInfo = logInfo
}

// This updates the shard manager to use new shard states.
func (m *BaseShardManager) UpdateShardStates(shardStates []ShardState) {
	newAddrs := set.NewSet()
	for _, state := range shardStates {
		newAddrs.Add(state.Address)
	}

	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	oldAddrs := set.NewSet()
	for _, state := range m.shardStates {
		oldAddrs.Add(state.Address)
	}

	for address := range set.Subtract(newAddrs, oldAddrs).Iter() {
		if err := m.pool.Register("tcp", address.(string)); err != nil {
			m.logError(err)
		}
	}

	for address := range set.Subtract(oldAddrs, newAddrs).Iter() {
		if err := m.pool.Unregister("tcp", address.(string)); err != nil {
			m.logError(err)
		}
	}

	m.shardStates = shardStates
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetShard(
	key string) (
	shardId int,
	conn net2.ManagedConn,
	err error) {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	shardId = m.getShardId(key, len(m.shardStates))
	if shardId == -1 {
		return
	}

	state := m.shardStates[shardId]
	if state.State != ActiveServer {
		m.logInfo("Memcache shard ", shardId, " is not in active state.")
		connSkippedByAddr.Add(state.Address, 1)
		return
	}

	entry := &ShardMapping{}
	m.fillEntryWithConnection(state.Address, entry)
	conn, err = entry.Connection, entry.ConnErr

	return
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetShardsForKeys(
	keys []string) map[int]*ShardMapping {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	numShards := len(m.shardStates)
	results := make(map[int]*ShardMapping)

	for _, key := range keys {
		shardId := m.getShardId(key, numShards)

		entry, inMap := results[shardId]
		if !inMap {
			entry = &ShardMapping{}
			if shardId != -1 {
				state := m.shardStates[shardId]
				if state.State == ActiveServer {
					m.fillEntryWithConnection(state.Address, entry)
				} else {
					connSkippedByAddr.Add(state.Address, 1)
				}
			}
			entry.Keys = make([]string, 0, 1)
			results[shardId] = entry
		}
		entry.Keys = append(entry.Keys, key)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetShardsForItems(
	items []*Item) map[int]*ShardMapping {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	numShards := len(m.shardStates)
	results := make(map[int]*ShardMapping)

	for _, item := range items {
		shardId := m.getShardId(item.Key, numShards)

		entry, inMap := results[shardId]
		if !inMap {
			entry = &ShardMapping{}
			if shardId != -1 {
				state := m.shardStates[shardId]
				if state.State == ActiveServer {
					m.fillEntryWithConnection(state.Address, entry)
				} else {
					connSkippedByAddr.Add(state.Address, 1)
				}
			}
			entry.Items = make([]*Item, 0, 1)
			results[shardId] = entry
		}
		entry.Items = append(entry.Items, item)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetShardsForSentinels(
	items []*Item) map[int]*ShardMapping {

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	numShards := len(m.shardStates)
	results := make(map[int]*ShardMapping)

	for _, item := range items {
		shardId := m.getShardId(item.Key, numShards)

		entry, inMap := results[shardId]
		if !inMap {
			entry = &ShardMapping{}
			if shardId != -1 {
				state := m.shardStates[shardId]
				if state.State == ActiveServer ||
					state.State == WriteOnlyServer ||
					state.State == WarmUpServer {

					m.fillEntryWithConnection(state.Address, entry)

					// During WARM_UP state, we do try to write sentinels to
					// memcache but any failures are ignored. We run memcache
					// server in this mode for sometime to prime our memcache
					// and warm up memcache server.
					if state.State == WarmUpServer {
						entry.WarmingUp = true
					}
				} else {
					connSkippedByAddr.Add(state.Address, 1)
				}
			}
			entry.Items = make([]*Item, 0, 1)
			results[shardId] = entry
		}
		entry.Items = append(entry.Items, item)
	}

	return results
}

// See ShardManager interface for documentation.
func (m *BaseShardManager) GetAllShards() map[int]net2.ManagedConn {
	results := make(map[int]net2.ManagedConn)

	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	for i, state := range m.shardStates {
		conn, err := m.pool.Get("tcp", state.Address)
		if err != nil {
			m.logError(err)
			conn = nil
		}
		results[i] = conn
	}

	return results
}

func (m *BaseShardManager) fillEntryWithConnection(address string, entry *ShardMapping) {
	conn, err := m.pool.Get("tcp", address)
	if err != nil {
		m.logError(err)
		connErrByAddr.Add(address, 1)
		entry.ConnErr = err
	} else {
		connOkByAddr.Add(address, 1)
		entry.Connection = conn
	}
}
