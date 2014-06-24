package memcache

import (
	"sync"

	"github.com/dropbox/godropbox/net2"
)

const (
	ActiveServer    = 0
	WriteOnlyServer = 1
	DownServer      = 2
)

type ShardState struct {
	address string
	state   int
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

	m.shardStates = make([]ShardState, 0, 0)
	m.getShardId = shardFunc
	m.pool = net2.NewMultiConnectionPool(options, nil)

	m.logError = logError
	m.logInfo = logInfo
}

// This updates the shard manager to use new shard states.
func (m *BaseShardManager) UpdateShardStates(shardStates []ShardState) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	// Register new connections / Unregister old connections
	diffs := make(map[string]int)
	for _, state := range m.shardStates {
		diffs[state.address] = -1
	}

	for _, state := range shardStates {
		diffs[state.address] += 1
	}

	for address, state := range diffs {
		if state == 1 { // New connections
			if err := m.pool.Register("tcp", address); err != nil {
				m.logError(err)
			}
		} else if state == -1 { // Old connections
			if err := m.pool.Unregister("tcp", address); err != nil {
				m.logError(err)
			}
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

	if m.shardStates[shardId].state != ActiveServer {
		m.logInfo("Memcache shard ", shardId, " is not in active state.")
		return
	}

	conn, err = m.pool.Get("tcp", m.shardStates[shardId].address)
	if err != nil {
		m.logError(err)
		conn = nil
	}

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
				if state.state == ActiveServer {
					conn, err := m.pool.Get("tcp", state.address)
					if err != nil {
						m.logError(err)
						entry.ConnErr = err
					} else {
						entry.Connection = conn
					}
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
				if state.state == ActiveServer {
					conn, err := m.pool.Get("tcp", state.address)
					if err != nil {
						m.logError(err)
						entry.ConnErr = err
					} else {
						entry.Connection = conn
					}
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
				if state.state == ActiveServer ||
					state.state == WriteOnlyServer {

					conn, err := m.pool.Get("tcp", state.address)
					if err != nil {
						m.logError(err)
						entry.ConnErr = err
					} else {
						entry.Connection = conn
					}
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
		conn, err := m.pool.Get("tcp", state.address)
		if err != nil {
			m.logError(err)
			conn = nil
		}
		results[i] = conn
	}

	return results
}
