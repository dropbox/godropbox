package memcache

import (
	"expvar"
	"github.com/dropbox/godropbox/errors"
	"github.com/dropbox/godropbox/net2"
)

// A sharded memcache client implementation where sharding management is
// handled by the provided ShardManager.
type ShardedClient struct {
	manager ShardManager
	builder ClientShardBuilder
}

var (
	// Counters for number of get requests that successed / errored, by address.
	getOkByAddr  = expvar.NewMap("ShardedClientGetOkByAddrCounter")
	getErrByAddr = expvar.NewMap("ShardedClientGetErrByAddrCounter")
)

// This creates a new ShardedClient.
func NewShardedClient(
	manager ShardManager,
	builder ClientShardBuilder) Client {

	return &ShardedClient{
		manager: manager,
		builder: builder,
	}
}

func (c *ShardedClient) release(rawClient ClientShard, conn net2.ManagedConn) {
	if rawClient.IsValidState() {
		conn.ReleaseConnection()
	} else {
		conn.DiscardConnection()
	}
}

func (c *ShardedClient) unmappedError(key string) error {
	return errors.Newf("Key '%s' does not map to any memcache shard", key)
}

func (c *ShardedClient) connectionError(shard int, err error) error {
	if err == nil {
		return errors.Newf(
			"Connection unavailable for memcache shard %d", shard)
	}
	return errors.Wrapf(
		err,
		"Connection unavailable for memcache shard %d", shard)
}

// See Client interface for documentation.
func (c *ShardedClient) Get(key string) GetResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewGetErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewGetErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
	}

	client := c.builder(shard, conn)
	defer c.release(client, conn)

	result := client.Get(key)
	if client.IsValidState() {
		getOkByAddr.Add(conn.Key().Address, 1)
	} else {
		getErrByAddr.Add(conn.Key().Address, 1)
	}
	return result
}

func (c *ShardedClient) getMultiHelper(
	shard int,
	conn net2.ManagedConn,
	connErr error,
	keys []string,
	resultsChannel chan map[string]GetResponse) {

	var results map[string]GetResponse
	if shard == -1 {
		results = make(map[string]GetResponse)
		for _, key := range keys {
			results[key] = NewGetErrorResponse(key, c.unmappedError(key))
		}
	} else if connErr != nil {
		results = make(map[string]GetResponse)
		for _, key := range keys {
			results[key] = NewGetErrorResponse(
				key,
				c.connectionError(shard, connErr))
		}
	} else if conn == nil {
		results = make(map[string]GetResponse)
		for _, key := range keys {
			// NOTE: zero is an invalid version id.
			results[key] = NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
		}
	} else {
		client := c.builder(shard, conn)
		defer c.release(client, conn)

		results = client.GetMulti(keys)
		if client.IsValidState() {
			getOkByAddr.Add(conn.Key().Address, 1)
		} else {
			getErrByAddr.Add(conn.Key().Address, 1)
		}
	}
	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) GetMulti(keys []string) map[string]GetResponse {
	return c.getMulti(c.manager.GetShardsForKeys(keys))
}

// See Client interface for documentation.
func (c *ShardedClient) GetSentinels(keys []string) map[string]GetResponse {
	return c.getMulti(c.manager.GetShardsForSentinelsFromKeys(keys))
}

func (c *ShardedClient) getMulti(shardMapping map[int]*ShardMapping) map[string]GetResponse {
	resultsChannel := make(chan map[string]GetResponse, len(shardMapping))
	for shard, mapping := range shardMapping {
		go c.getMultiHelper(
			shard,
			mapping.Connection,
			mapping.ConnErr,
			mapping.Keys,
			resultsChannel)
	}

	results := make(map[string]GetResponse)
	for i := 0; i < len(shardMapping); i++ {
		for key, resp := range <-resultsChannel {
			results[key] = resp
		}
	}
	return results
}

func (c *ShardedClient) mutate(
	key string,
	mutateFunc func(Client) MutateResponse) MutateResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewMutateErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewMutateErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(key, StatusNoError, 0)
	}

	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return mutateFunc(client)
}

// See Client interface for documentation.
func (c *ShardedClient) Set(item *Item) MutateResponse {
	return c.mutate(
		item.Key,
		func(shardClient Client) MutateResponse {
			return shardClient.Set(item)
		})
}

func (c *ShardedClient) mutateMultiHelper(
	mutateMultiFunc func(Client, *ShardMapping) []MutateResponse,
	shard int,
	mapping *ShardMapping,
	resultsChannel chan []MutateResponse) {

	keys := mapping.Keys
	conn := mapping.Connection
	connErr := mapping.ConnErr
	warmingUp := mapping.WarmingUp

	var results []MutateResponse
	if shard == -1 {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			results = append(
				results,
				NewMutateErrorResponse(key, c.unmappedError(key)))
		}
	} else if connErr != nil {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			results = append(
				results,
				NewMutateErrorResponse(key, c.connectionError(shard, connErr)))
		}
	} else if conn == nil {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			// NOTE: zero is an invalid version id.
			results = append(
				results,
				NewMutateResponse(key, StatusNoError, 0))
		}
	} else {
		client := c.builder(shard, conn)
		defer c.release(client, conn)

		results = mutateMultiFunc(client, mapping)
	}

	// If server is warming up, we override all failures with success message.
	if warmingUp {
		for idx, key := range keys {
			if results[idx].Error() != nil {
				results[idx] = NewMutateResponse(key, StatusNoError, 0)
			}
		}
	}

	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) mutateMulti(
	shards map[int]*ShardMapping,
	mutateMultiFunc func(Client, *ShardMapping) []MutateResponse) []MutateResponse {

	numKeys := 0

	resultsChannel := make(chan []MutateResponse, len(shards))
	for shard, mapping := range shards {
		numKeys += len(mapping.Keys)
		go c.mutateMultiHelper(
			mutateMultiFunc,
			shard,
			mapping,
			resultsChannel)
	}

	results := make([]MutateResponse, 0, numKeys)
	for i := 0; i < len(shards); i++ {
		results = append(results, (<-resultsChannel)...)
	}
	return results
}

// A helper used to specify a SetMulti mutation operation on a shard client.
func setMultiMutator(shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.SetMulti(mapping.Items)
}

// A helper used to specify a CasMulti mutation operation on a shard client.
func casMultiMutator(shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.CasMulti(mapping.Items)
}

// See Client interface for documentation.
func (c *ShardedClient) SetMulti(items []*Item) []MutateResponse {
	return c.mutateMulti(c.manager.GetShardsForItems(items), setMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) SetSentinels(items []*Item) []MutateResponse {
	return c.mutateMulti(c.manager.GetShardsForSentinelsFromItems(items), setMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) CasMulti(items []*Item) []MutateResponse {
	return c.mutateMulti(c.manager.GetShardsForItems(items), casMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) CasSentinels(items []*Item) []MutateResponse {
	return c.mutateMulti(c.manager.GetShardsForSentinelsFromItems(items), casMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Add(item *Item) MutateResponse {
	return c.mutate(
		item.Key,
		func(shardClient Client) MutateResponse {
			return shardClient.Add(item)
		})
}

// A helper used to specify a AddMulti mutation operation on a shard client.
func addMultiMutator(shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.AddMulti(mapping.Items)
}

// See Client interface for documentation.
func (c *ShardedClient) AddMulti(items []*Item) []MutateResponse {
	return c.mutateMulti(c.manager.GetShardsForItems(items), addMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Replace(item *Item) MutateResponse {
	return c.mutate(
		item.Key,
		func(shardClient Client) MutateResponse {
			return shardClient.Replace(item)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Delete(key string) MutateResponse {
	return c.mutate(
		key,
		func(shardClient Client) MutateResponse {
			return shardClient.Delete(key)
		})
}

// A helper used to specify a DeleteMulti mutation operation on a shard client.
func deleteMultiMutator(shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.DeleteMulti(mapping.Keys)
}

// See Client interface for documentation.
func (c *ShardedClient) DeleteMulti(keys []string) []MutateResponse {
	return c.mutateMulti(c.manager.GetShardsForKeys(keys), deleteMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Append(key string, value []byte) MutateResponse {
	return c.mutate(
		key,
		func(shardClient Client) MutateResponse {
			return shardClient.Append(key, value)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Prepend(key string, value []byte) MutateResponse {
	return c.mutate(
		key,
		func(shardClient Client) MutateResponse {
			return shardClient.Prepend(key, value)
		})
}

func (c *ShardedClient) count(
	key string,
	countFunc func(Client) CountResponse) CountResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewCountErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewCountErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		return NewCountResponse(key, StatusNoError, 0)
	}

	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return countFunc(client)
}

// See Client interface for documentation.
func (c *ShardedClient) Increment(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.count(
		key,
		func(shardClient Client) CountResponse {
			return shardClient.Increment(key, delta, initValue, expiration)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Decrement(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.count(
		key,
		func(shardClient Client) CountResponse {
			return shardClient.Decrement(key, delta, initValue, expiration)
		})
}

func (c *ShardedClient) flushHelper(
	shard int,
	conn net2.ManagedConn,
	expiration uint32) Response {

	if conn == nil {
		return NewErrorResponse(c.connectionError(shard, nil))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Flush(expiration)
}

// See Client interface for documentation.
func (c *ShardedClient) Flush(expiration uint32) Response {
	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.flushHelper(shard, conn, expiration)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
		}
	}

	if err != nil {
		return NewErrorResponse(err)
	}

	return NewResponse(StatusNoError)
}

func (c *ShardedClient) statHelper(
	shard int,
	conn net2.ManagedConn,
	statsKey string) StatResponse {

	if conn == nil {
		return NewStatErrorResponse(
			c.connectionError(shard, nil),
			make(map[int](map[string]string)))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Stat(statsKey)
}

// See Client interface for documentation.
func (c *ShardedClient) Stat(statsKey string) StatResponse {
	statEntries := make(map[int](map[string]string))

	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.statHelper(shard, conn, statsKey)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
		}

		for shardId, entries := range response.Entries() {
			statEntries[shardId] = entries
		}
	}

	if err != nil {
		return NewStatErrorResponse(err, statEntries)
	}

	return NewStatResponse(StatusNoError, statEntries)
}

func (c *ShardedClient) versionHelper(
	shard int,
	conn net2.ManagedConn) VersionResponse {

	if conn == nil {
		return NewVersionErrorResponse(
			c.connectionError(shard, nil),
			make(map[int]string))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Version()
}

// See Client interface for documentation.
func (c *ShardedClient) Version() VersionResponse {
	shardConns := c.manager.GetAllShards()

	var err error
	versions := make(map[int]string)
	for shard, conn := range shardConns {
		response := c.versionHelper(shard, conn)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
			continue
		}

		for shardId, versionString := range response.Versions() {
			versions[shardId] = versionString
		}
	}

	if err != nil {
		return NewVersionErrorResponse(err, versions)
	}

	return NewVersionResponse(StatusNoError, versions)
}

func (c *ShardedClient) verbosityHelper(
	shard int,
	conn net2.ManagedConn,
	verbosity uint32) Response {

	if conn == nil {
		return NewErrorResponse(c.connectionError(shard, nil))
	}
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Verbosity(verbosity)
}

// See Client interface for documentation.
func (c *ShardedClient) Verbosity(verbosity uint32) Response {
	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.verbosityHelper(shard, conn, verbosity)
		if response.Error() != nil {
			if err == nil {
				err = response.Error()
			} else {
				err = errors.Wrap(response.Error(), err.Error())
			}
		}
	}

	if err != nil {
		return NewErrorResponse(err)
	}

	return NewResponse(StatusNoError)
}
