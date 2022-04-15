package memcache

import (
	"context"
	"expvar"
	"godropbox/errors"
	"godropbox/net2"
)

// A sharded memcache client implementation where sharding management is
// handled by the provided ShardManager.
type ShardedClient struct {
	manager ShardManager
	builder ClientShardBuilder
}

var (
	// Counters for number of get requests that successed / errored, by shard.
	getOkByShard  = expvar.NewMap("ShardedClientGetOkByShardCounter")
	getErrByShard = expvar.NewMap("ShardedClientGetErrByShardCounter")
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
		_ = conn.ReleaseConnection()
	} else {
		_ = conn.DiscardConnection()
	}
}

// See Client interface for documentation.
func (c *ShardedClient) Get(ctx context.Context, key string) GetResponse {
	conn, err := c.manager.GetShard(key)
	if err != nil {
		return NewGetErrorResponse(key, err)
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
	}

	shard := conn.Key().Address
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	result := client.Get(ctx, key)
	if client.IsValidState() {
		getOkByShard.Add(shard, 1)
	} else {
		getErrByShard.Add(shard, 1)
	}
	return result
}

func (c *ShardedClient) getMultiHelper(
	ctx context.Context,
	shard string,
	conn net2.ManagedConn,
	connErr error,
	keys []string,
	resultsChannel chan map[string]GetResponse) {

	var results map[string]GetResponse
	if shard == "" {
		results = make(map[string]GetResponse, len(keys))
		for _, key := range keys {
			results[key] = NewGetErrorResponse(key, noShardsError(key))
		}
	} else if connErr != nil {
		results = make(map[string]GetResponse, len(keys))
		for _, key := range keys {
			results[key] = NewGetErrorResponse(
				key,
				connectionError(shard, connErr))
		}
	} else if conn == nil {
		results = make(map[string]GetResponse, len(keys))
		for _, key := range keys {
			// NOTE: zero is an invalid version id.
			results[key] = NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
		}
	} else {
		// shard == conn.Key().Address
		client := c.builder(shard, conn)
		defer c.release(client, conn)

		results = client.GetMulti(ctx, keys)
		if client.IsValidState() {
			getOkByShard.Add(shard, 1)
		} else {
			getErrByShard.Add(shard, 1)
		}
	}
	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) GetMulti(ctx context.Context, keys []string) map[string]GetResponse {
	return c.getMulti(ctx, c.manager.GetShardsForKeys(keys))
}

func (c *ShardedClient) getMulti(ctx context.Context, shardMapping map[string]*ShardMapping) map[string]GetResponse {
	resultsChannel := make(chan map[string]GetResponse, len(shardMapping))
	for shard, mapping := range shardMapping {
		go c.getMultiHelper(
			ctx,
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
	conn, err := c.manager.GetShard(key)
	if err != nil {
		return NewMutateErrorResponse(key, err)
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(key, StatusNoError, 0)
	}

	shard := conn.Key().Address
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return mutateFunc(client)
}

// See Client interface for documentation.
func (c *ShardedClient) Set(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(
		item.Key,
		func(shardClient Client) MutateResponse {
			return shardClient.Set(ctx, item)
		})
}

func (c *ShardedClient) mutateMultiHelper(
	ctx context.Context,
	mutateMultiFunc func(context.Context, Client, *ShardMapping) []MutateResponse,
	shard string,
	mapping *ShardMapping,
	resultsChannel chan []MutateResponse) {

	keys := mapping.Keys
	conn := mapping.Connection
	connErr := mapping.ConnErr

	var results []MutateResponse
	if shard == "" {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			results = append(
				results,
				NewMutateErrorResponse(key, noShardsError(key)))
		}
	} else if connErr != nil {
		results = make([]MutateResponse, 0, len(keys))
		for _, key := range keys {
			results = append(
				results,
				NewMutateErrorResponse(key, connectionError(shard, connErr)))
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
		// shard == conn.Key().Address
		client := c.builder(shard, conn)
		defer c.release(client, conn)

		results = mutateMultiFunc(ctx, client, mapping)
	}

	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) mutateMulti(
	ctx context.Context,
	shards map[string]*ShardMapping,
	mutateMultiFunc func(context.Context, Client, *ShardMapping) []MutateResponse,
) []MutateResponse {

	numKeys := 0

	resultsChannel := make(chan []MutateResponse, len(shards))
	for shard, mapping := range shards {
		numKeys += len(mapping.Keys)
		go c.mutateMultiHelper(
			ctx,
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
func setMultiMutator(ctx context.Context, shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.SetMulti(ctx, mapping.Items)
}

// A helper used to specify a CasMulti mutation operation on a shard client.
func casMultiMutator(ctx context.Context, shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.CasMulti(ctx, mapping.Items)
}

// See Client interface for documentation.
func (c *ShardedClient) SetMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForItems(items), setMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) CasMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForItems(items), casMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Add(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(
		item.Key,
		func(shardClient Client) MutateResponse {
			return shardClient.Add(ctx, item)
		})
}

// A helper used to specify a AddMulti mutation operation on a shard client.
func addMultiMutator(ctx context.Context, shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.AddMulti(ctx, mapping.Items)
}

// See Client interface for documentation.
func (c *ShardedClient) AddMulti(ctx context.Context, items []*Item) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForItems(items), addMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Replace(ctx context.Context, item *Item) MutateResponse {
	return c.mutate(
		item.Key,
		func(shardClient Client) MutateResponse {
			return shardClient.Replace(ctx, item)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Delete(ctx context.Context, key string) MutateResponse {
	return c.mutate(
		key,
		func(shardClient Client) MutateResponse {
			return shardClient.Delete(ctx, key)
		})
}

// A helper used to specify a DeleteMulti mutation operation on a shard client.
func deleteMultiMutator(ctx context.Context, shardClient Client, mapping *ShardMapping) []MutateResponse {
	return shardClient.DeleteMulti(ctx, mapping.Keys)
}

// See Client interface for documentation.
func (c *ShardedClient) DeleteMulti(ctx context.Context, keys []string) []MutateResponse {
	return c.mutateMulti(ctx, c.manager.GetShardsForKeys(keys), deleteMultiMutator)
}

// See Client interface for documentation.
func (c *ShardedClient) Append(ctx context.Context, key string, value []byte) MutateResponse {
	return c.mutate(
		key,
		func(shardClient Client) MutateResponse {
			return shardClient.Append(ctx, key, value)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Prepend(ctx context.Context, key string, value []byte) MutateResponse {
	return c.mutate(
		key,
		func(shardClient Client) MutateResponse {
			return shardClient.Prepend(ctx, key, value)
		})
}

func (c *ShardedClient) count(
	key string,
	countFunc func(Client) CountResponse) CountResponse {
	conn, err := c.manager.GetShard(key)
	if err != nil {
		return NewCountErrorResponse(key, err)
	}
	if conn == nil {
		return NewCountResponse(key, StatusNoError, 0)
	}

	client := c.builder(conn.Key().Address, conn)
	defer c.release(client, conn)

	return countFunc(client)
}

// See Client interface for documentation.
func (c *ShardedClient) Increment(
	ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.count(
		key,
		func(shardClient Client) CountResponse {
			return shardClient.Increment(ctx, key, delta, initValue, expiration)
		})
}

// See Client interface for documentation.
func (c *ShardedClient) Decrement(
	ctx context.Context,
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return c.count(
		key,
		func(shardClient Client) CountResponse {
			return shardClient.Decrement(ctx, key, delta, initValue, expiration)
		})
}

func (c *ShardedClient) flushHelper(
	ctx context.Context,
	shard string,
	conn net2.ManagedConn,
	expiration uint32) Response {

	if conn == nil {
		return NewErrorResponse(connectionError(shard, nil))
	}
	// shard == conn.Key().Address
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Flush(ctx, expiration)
}

// See Client interface for documentation.
func (c *ShardedClient) Flush(ctx context.Context, expiration uint32) Response {
	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.flushHelper(ctx, shard, conn, expiration)
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
	ctx context.Context,
	shard string,
	conn net2.ManagedConn,
	statsKey string) StatResponse {

	if conn == nil {
		return NewStatErrorResponse(
			connectionError(shard, nil),
			make(map[string](map[string]string)))
	}
	// shard == conn.Key().Address
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Stat(ctx, statsKey)
}

// See Client interface for documentation.
func (c *ShardedClient) Stat(ctx context.Context, statsKey string) StatResponse {
	statEntries := make(map[string](map[string]string))

	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.statHelper(ctx, shard, conn, statsKey)
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
	ctx context.Context,
	shard string,
	conn net2.ManagedConn) VersionResponse {

	if conn == nil {
		return NewVersionErrorResponse(
			connectionError(shard, nil),
			make(map[string]string))
	}
	// shard == conn.Key().Address
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Version(ctx)
}

// See Client interface for documentation.
func (c *ShardedClient) Version(ctx context.Context) VersionResponse {
	shardConns := c.manager.GetAllShards()

	var err error
	versions := make(map[string]string)
	for shard, conn := range shardConns {
		response := c.versionHelper(ctx, shard, conn)
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
	ctx context.Context,
	shard string,
	conn net2.ManagedConn,
	verbosity uint32) Response {

	if conn == nil {
		return NewErrorResponse(connectionError(shard, nil))
	}
	// shard == conn.Key().Address
	client := c.builder(shard, conn)
	defer c.release(client, conn)

	return client.Verbosity(ctx, verbosity)
}

// See Client interface for documentation.
func (c *ShardedClient) Verbosity(ctx context.Context, verbosity uint32) Response {
	var err error
	for shard, conn := range c.manager.GetAllShards() {
		response := c.verbosityHelper(ctx, shard, conn, verbosity)
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
