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

	// When true, use ascii protocol.  Otherwise, use binary protocol.
	useAsciiProtocol bool
}

var (
	// Counters for number of get requests that successed / errored, by address.
	getOkByAddr  = expvar.NewMap("ShardedClientGetOkByAddrCounter")
	getErrByAddr = expvar.NewMap("ShardedClientGetErrByAddrCounter")
)

// This creates a new ShardedClient.
func NewShardedClient(
	manager ShardManager,
	useAsciiProtocol bool) Client {

	return &ShardedClient{
		manager:          manager,
		useAsciiProtocol: useAsciiProtocol,
	}
}

func (s *ShardedClient) newRawClient(
	shard int,
	conn net2.ManagedConn) ClientShard {

	if s.useAsciiProtocol {
		return NewRawAsciiClient(shard, conn)
	}
	return NewRawBinaryClient(shard, conn)
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

	client := c.newRawClient(shard, conn)
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
		client := c.newRawClient(shard, conn)
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
	shardMapping := c.manager.GetShardsForKeys(keys)

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
	mutateFunc func(Client, *Item) MutateResponse,
	item *Item) MutateResponse {
	shard, conn, err := c.manager.GetShard(item.Key)
	if shard == -1 {
		return NewMutateErrorResponse(item.Key, c.unmappedError(item.Key))
	}
	if err != nil {
		return NewMutateErrorResponse(item.Key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(item.Key, StatusNoError, 0, c.useAsciiProtocol)
	}

	client := c.newRawClient(shard, conn)
	defer c.release(client, conn)

	return mutateFunc(client, item)
}

// A helper used to specify a set mutation operation on a shard client.
func setMutator(shardClient Client, shardItem *Item) MutateResponse {
	return shardClient.Set(shardItem)
}

// See Client interface for documentation.
func (c *ShardedClient) Set(item *Item) MutateResponse {
	return c.mutate(setMutator, item)
}

func (c *ShardedClient) mutateMultiHelper(
	mutateMultiFunc func(Client, []*Item) []MutateResponse,
	shard int,
	conn net2.ManagedConn,
	connErr error,
	items []*Item,
	warmingUp bool,
	resultsChannel chan []MutateResponse) {

	var results []MutateResponse
	if shard == -1 {
		results = make([]MutateResponse, 0, len(items))
		for _, item := range items {
			results = append(
				results,
				NewMutateErrorResponse(item.Key, c.unmappedError(item.Key)))
		}
	} else if connErr != nil {
		results = make([]MutateResponse, 0, len(items))
		for _, item := range items {
			results = append(
				results,
				NewMutateErrorResponse(
					item.Key,
					c.connectionError(shard, connErr)))
		}
	} else if conn == nil {
		results = make([]MutateResponse, 0, len(items))
		for _, item := range items {
			// NOTE: zero is an invalid version id.
			results = append(
				results,
				NewMutateResponse(
					item.Key,
					StatusNoError,
					0,
					c.useAsciiProtocol))
		}
	} else {
		client := c.newRawClient(shard, conn)
		defer c.release(client, conn)

		results = mutateMultiFunc(client, items)
	}

	// If server is warming up, we override all failures with success message.
	if warmingUp {
		for idx, item := range items {
			if results[idx].Error() != nil {
				results[idx] = NewMutateResponse(
					item.Key,
					StatusNoError,
					0,
					c.useAsciiProtocol)
			}
		}
	}

	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) mutateMulti(
	mutateMultiFunc func(Client, []*Item) []MutateResponse,
	items []*Item) []MutateResponse {
	shardMapping := c.manager.GetShardsForItems(items)

	resultsChannel := make(chan []MutateResponse, len(shardMapping))
	for shard, mapping := range shardMapping {
		go c.mutateMultiHelper(
			mutateMultiFunc,
			shard,
			mapping.Connection,
			mapping.ConnErr,
			mapping.Items,
			false,
			resultsChannel)
	}

	results := make([]MutateResponse, 0, len(items))
	for i := 0; i < len(shardMapping); i++ {
		results = append(results, (<-resultsChannel)...)
	}
	return results
}

// A helper used to specify a SetMulti mutation operation on a shard client.
func setMultiMutator(shardClient Client, shardItems []*Item) []MutateResponse {
	return shardClient.SetMulti(shardItems)
}

// See Client interface for documentation.
func (c *ShardedClient) SetMulti(items []*Item) []MutateResponse {
	return c.mutateMulti(setMultiMutator, items)
}

// See Client interface for documentation.
func (c *ShardedClient) SetSentinels(items []*Item) []MutateResponse {
	shardMapping := c.manager.GetShardsForSentinels(items)

	resultsChannel := make(chan []MutateResponse, len(shardMapping))
	for shard, mapping := range shardMapping {
		go c.mutateMultiHelper(
			setMultiMutator,
			shard,
			mapping.Connection,
			mapping.ConnErr,
			mapping.Items,
			mapping.WarmingUp,
			resultsChannel)
	}

	results := make([]MutateResponse, 0, len(items))
	for i := 0; i < len(shardMapping); i++ {
		results = append(results, (<-resultsChannel)...)
	}
	return results
}

// A helper used to specify an Add mutation operation on a shard client.
func addMutator(shardClient Client, shardItem *Item) MutateResponse {
	return shardClient.Add(shardItem)
}

// See Client interface for documentation.
func (c *ShardedClient) Add(item *Item) MutateResponse {
	return c.mutate(addMutator, item)
}

// A helper used to specify a AddMulti mutation operation on a shard client.
func addMultiMutator(shardClient Client, shardItems []*Item) []MutateResponse {
	return shardClient.AddMulti(shardItems)
}

// See Client interface for documentation.
func (c *ShardedClient) AddMulti(items []*Item) []MutateResponse {
	return c.mutateMulti(addMultiMutator, items)
}

// See Client interface for documentation.
func (c *ShardedClient) Replace(item *Item) MutateResponse {
	shard, conn, err := c.manager.GetShard(item.Key)
	if shard == -1 {
		return NewMutateErrorResponse(item.Key, c.unmappedError(item.Key))
	}
	if err != nil {
		return NewMutateErrorResponse(
			item.Key,
			c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(item.Key, StatusNoError, 0, c.useAsciiProtocol)
	}

	client := c.newRawClient(shard, conn)
	defer c.release(client, conn)

	return client.Replace(item)
}

// See Client interface for documentation.
func (c *ShardedClient) Delete(key string) MutateResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewMutateErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewMutateErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(key, StatusNoError, 0, c.useAsciiProtocol)
	}

	client := c.newRawClient(shard, conn)
	defer c.release(client, conn)

	return client.Delete(key)
}

func (c *ShardedClient) deleteMultiHelper(
	shard int,
	conn net2.ManagedConn,
	connErr error,
	keys []string,
	resultsChannel chan []MutateResponse) {

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
				NewMutateResponse(key, StatusNoError, 0, c.useAsciiProtocol))
		}
	} else {
		client := c.newRawClient(shard, conn)
		defer c.release(client, conn)

		results = client.DeleteMulti(keys)
	}
	resultsChannel <- results
}

// See Client interface for documentation.
func (c *ShardedClient) DeleteMulti(keys []string) []MutateResponse {
	shardMapping := c.manager.GetShardsForKeys(keys)

	resultsChannel := make(chan []MutateResponse, len(shardMapping))
	for shard, mapping := range shardMapping {
		go c.deleteMultiHelper(
			shard,
			mapping.Connection,
			mapping.ConnErr,
			mapping.Keys,
			resultsChannel)
	}

	results := make([]MutateResponse, 0, len(keys))
	for i := 0; i < len(shardMapping); i++ {
		results = append(results, (<-resultsChannel)...)
	}
	return results
}

// See Client interface for documentation.
func (c *ShardedClient) Append(key string, value []byte) MutateResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewMutateErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewMutateErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(key, StatusNoError, 0, c.useAsciiProtocol)
	}

	client := c.newRawClient(shard, conn)
	defer c.release(client, conn)

	return client.Append(key, value)
}

// See Client interface for documentation.
func (c *ShardedClient) Prepend(key string, value []byte) MutateResponse {
	shard, conn, err := c.manager.GetShard(key)
	if shard == -1 {
		return NewMutateErrorResponse(key, c.unmappedError(key))
	}
	if err != nil {
		return NewMutateErrorResponse(key, c.connectionError(shard, err))
	}
	if conn == nil {
		// NOTE: zero is an invalid version id.
		return NewMutateResponse(key, StatusNoError, 0, c.useAsciiProtocol)
	}

	client := c.newRawClient(shard, conn)
	defer c.release(client, conn)

	return client.Prepend(key, value)
}

// See Client interface for documentation.
func (c *ShardedClient) Increment(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

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

	client := c.newRawClient(shard, conn)
	defer c.release(client, conn)

	return client.Increment(key, delta, initValue, expiration)
}

// See Client interface for documentation.
func (c *ShardedClient) Decrement(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

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

	client := c.newRawClient(shard, conn)
	defer c.release(client, conn)

	return client.Decrement(key, delta, initValue, expiration)
}

func (c *ShardedClient) flushHelper(
	shard int,
	conn net2.ManagedConn,
	expiration uint32) Response {

	if conn == nil {
		return NewErrorResponse(c.connectionError(shard, nil))
	}
	client := c.newRawClient(shard, conn)
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
	client := c.newRawClient(shard, conn)
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
	client := c.newRawClient(shard, conn)
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
	client := c.newRawClient(shard, conn)
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
