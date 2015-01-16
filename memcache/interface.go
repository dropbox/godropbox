package memcache

import (
	"github.com/dropbox/godropbox/net2"
)

// An item to be gotten from or stored in a memcache server.
type Item struct {
	// The item's key (the key can be up to 250 bytes maximum).
	Key string

	// The item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely up to the app.
	Flags uint32

	// aka CAS (check and set) in memcache documentation.
	DataVersionId uint64

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration uint32
}

// A generic response to a memcache request.
type Response interface {
	// This returns the status returned by the memcache server.  When Error()
	// is non-nil, this value may not be valid.
	//
	// NOTE:
	// 1. For stat request, this returns the first non-StatusNoError encountered
	//    (or StatusNoError if there were no errors).
	// 2. If the client is sharded, flush/stats/version/verbosity requests
	//    will return the first non-StatusNoError encountered (or StatusNoError
	//    if there were no errors).
	Status() ResponseStatus

	// This returns nil when no error is encountered by the client, and the
	// response status returned by the memcache server is StatusNoError.
	// Otherwise, this returns an error.
	//
	// NOTE:
	// 1. For get requests, this also returns nil when the response status
	//    StatusKeyNotFound.
	// 2. For stat request, this returns the first error encountered (or nil
	//    if there were no errors).
	// 3. If the client is sharded, flush/stats/version/verbosity requests
	//    will return the first error encountered (or nil if there were no
	//    errors).
	Error() error
}

// Response returned by Get/GetKey/GetAndTouch requests.
type GetResponse interface {
	Response

	// This returns the key for the requested value.
	Key() string

	// This returns the retreived entry.  The value may be nil.
	Value() []byte

	// This returns the entry's flags value.  The value is only valid when
	// the entry is found.
	Flags() uint32

	// This returns the data version id (aka CAS) for the item.  The value is
	// only valid when the entry is found.
	DataVersionId() uint64
}

// Response returned by Set/Add/Replace/Delete/Append/Prepend requests.
type MutateResponse interface {
	Response

	// This returns the input key (useful for SetMulti where operations may be
	// applied out of order).
	Key() string

	// This returns the data version id (aka CAS) for the item.  For delete
	// requests, this always returns zero.
	DataVersionId() uint64
}

// Response returned by Increment/Decrement requests.
type CountResponse interface {
	Response

	// This returns the input key (useful for SetMulti where operations may be
	// applied out of order).
	Key() string

	// This returns the resulting count value.  On error status, this returns
	// zero.
	Count() uint64
}

// Response returned by Version request.
type VersionResponse interface {
	Response

	// This returns the memcache version entries.  On error status, this
	// returns an empty string.  The mapping is stored as:
	//      shard id -> version string
	// (If the client is unsharded, the shard id is always zero).
	Versions() map[int]string
}

// Response returned by Stat request.
type StatResponse interface {
	Response

	// This returns the retrieved stat entries.  On error status, this returns
	// nil.  The mapping is stored as:
	//      shard id -> stats key -> stats value
	// (If the client is unsharded, the shard id is always zero).
	Entries() map[int](map[string]string)
}

type Client interface {
	// This retrieves a single entry from memcache.
	Get(key string) GetResponse

	// Batch version of the Get method.
	GetMulti(keys []string) map[string]GetResponse

	// This sets a single entry into memcache.  If the item's data version id
	// (aka CAS) is nonzero, the set operation can only succeed if the item
	// exists in memcache and has a same data version id.
	Set(item *Item) MutateResponse

	// Batch version of the Set method.  Note that the response entries
	// ordering is undefined (i.e., may not match the input ordering).
	SetMulti(items []*Item) []MutateResponse

	// *** This method is specific to Dropbox zookeeper-managed memcache ***
	// This is the same as SetMutli.  The only difference is that SetMulti will
	// only write to ACTIVE memcache shards, while SetSentinels will write to
	// both ACTIVE and WRITE_ONLY memcache shards.
	SetSentinels(items []*Item) []MutateResponse

	// This adds a single entry into memcache.  Note: Add will fail if the
	// item already exist in memcache.
	Add(item *Item) MutateResponse

	// Batch version of the Add method.  Note that the response entries
	// ordering is undefined (i.e., may not match the input ordering).
	AddMulti(item []*Item) []MutateResponse

	// This replaces a single entry in memcache.  Note: Replace will fail if
	// the does not exist in memcache.
	Replace(item *Item) MutateResponse

	// This delets a single entry from memcache.
	Delete(key string) MutateResponse

	// Batch version of the Delete method.  Note that the response entries
	// ordering is undefined (i.e., may not match the input ordering)
	DeleteMulti(keys []string) []MutateResponse

	// This appends the value bytes to the end of an existing entry.  Note that
	// this does not allow you to extend past the item limit.
	Append(key string, value []byte) MutateResponse

	// This prepends the value bytes to the end of an existing entry.  Note that
	// this does not allow you to extend past the item limit.
	Prepend(key string, value []byte) MutateResponse

	// This increments the key's counter by delta.  If the counter does not
	// exist, one of two things may happen:
	// 1. If the expiration value is all one-bits (0xffffffff), the operation
	//    will fail with StatusNotFound.
	// 2. For all other expiration values, the operation will succeed by
	//    seeding the value for this key with the provided initValue to expire
	//    with the provided expiration time. The flags will be set to zero.
	//
	// NOTE:
	// 1. If you want to set the value of the counter with add/set/replace,
	//    the objects data must be the ascii representation of the value and
	//    not the byte values of a 64 bit integer.
	// 2. Incrementing the counter may cause the counter to wrap.
	Increment(
		key string,
		delta uint64,
		initValue uint64,
		expiration uint32) CountResponse

	// This decrements the key's counter by delta.  If the counter does not
	// exist, one of two things may happen:
	// 1. If the expiration value is all one-bits (0xffffffff), the operation
	//    will fail with StatusNotFound.
	// 2. For all other expiration values, the operation will succeed by
	//    seeding the value for this key with the provided initValue to expire
	//    with the provided expiration time. The flags will be set to zero.
	//
	// NOTE:
	// 1. If you want to set the value of the counter with add/set/replace,
	//    the objects data must be the ascii representation of the value and
	//    not the byte values of a 64 bit integer.
	// 2. Decrementing a counter will never result in a "negative value" (or
	//    cause the counter to "wrap"). instead the counter is set to 0.
	Decrement(
		key string,
		delta uint64,
		initValue uint64,
		expiration uint32) CountResponse

	// This invalidates all existing cache items after expiration number of
	// seconds.
	Flush(expiration uint32) Response

	// This requests the server statistics. When the key is an empty string,
	// the server will respond with a "default" set of statistics information.
	Stat(statsKey string) StatResponse

	// This returns the server's version string.
	Version() VersionResponse

	// This set the verbosity level of the server.
	Verbosity(verbosity uint32) Response
}

// A memcache client which communicates with a specific memcache shard.
type ClientShard interface {
	Client

	// This returns the memcache server's shard id.
	ShardId() int

	// This returns true if the client is in a valid state.  If the client is
	// in invalid state, the user should abandon the current client / channel,
	// and create a new client / channel as replacment.
	IsValidState() bool
}

// Used for returning shard mapping results from ShardManager's
// GetShardsForKeys/GetShardsForItems calls.
type ShardMapping struct {
	Connection net2.ManagedConn
	ConnErr    error
	Keys       []string // Populated for GetShardsForKeys
	Items      []*Item  // Populated for GetShardsForItems
	WarmingUp  bool     // Populated for GetShardsForSentinels
}

// The ShardManager decides which memcache shard a key/item belongs to, and
// provide connections to the shards.
type ShardManager interface {
	// This returns the shard id and a connection to shard for a single key.
	// If the key does not belong to any shard, this returns (-1, nil). Note
	// that the connection may be nil for valid shard id (for instance, if
	// the shard server is temporarily unavailable).
	GetShard(key string) (shardId int, conn net2.ManagedConn, err error)

	// This returns a (shard id -> (connection, list of keys)) mapping for
	// the requested keys.  Keys that do not belong to any shard are mapped
	// to shard id -1.
	GetShardsForKeys(keys []string) map[int]*ShardMapping

	// This returns a (shard id -> (connection, list of items)) mapping for
	// the requested items.  Items that do not belong to any shard are mapped
	// to shard id -1.
	GetShardsForItems(items []*Item) map[int]*ShardMapping

	// *** This method is specific to Dropbox zookeeper-managed memcache ***
	// This returns a (shard id -> (connection, list of items)) mapping for
	// the requested sentinel items.  Sential items that do not belong to any
	// shard are mapped to shard id -1.
	GetShardsForSentinels(items []*Item) map[int]*ShardMapping

	// This return a (shard id -> connection) mapping for all shards.
	GetAllShards() map[int]net2.ManagedConn
}
