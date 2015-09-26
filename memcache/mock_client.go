package memcache

import (
	"sync"

	"github.com/dropbox/godropbox/errors"
)

type MockClient struct {
	data    map[string]*Item
	version uint64
	mutex   sync.Mutex
}

func NewMockClient() Client {
	return &MockClient{data: make(map[string]*Item)}
}

func (c *MockClient) getHelper(key string) GetResponse {
	if v, ok := c.data[key]; ok {
		return NewGetResponse(
			key,
			StatusNoError,
			v.Flags,
			v.Value,
			v.DataVersionId)
	}
	return NewGetResponse(key, StatusKeyNotFound, 0, nil, 0)
}

// This retrieves a single entry from memcache.
func (c *MockClient) Get(key string) GetResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.getHelper(key)
}

// Batch version of the Get method.
func (c *MockClient) GetMulti(keys []string) map[string]GetResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := make(map[string]GetResponse)
	for _, key := range keys {
		res[key] = c.getHelper(key)
	}
	return res
}

func (c *MockClient) setHelper(item *Item) MutateResponse {
	c.version++

	newItem := &Item{
		Key:           item.Key,
		Value:         item.Value,
		Flags:         item.Flags,
		Expiration:    item.Expiration,
		DataVersionId: c.version,
	}

	existing, ok := c.data[newItem.Key]

	if item.DataVersionId == 0 ||
		(ok && item.DataVersionId == existing.DataVersionId) {

		c.data[newItem.Key] = newItem
		return NewMutateResponse(
			newItem.Key,
			StatusNoError,
			newItem.DataVersionId,
			false)
	} else if !ok {
		return NewMutateResponse(
			newItem.Key,
			StatusKeyNotFound,
			0,
			false)
	} else {
		// CAS mismatch
		return NewMutateResponse(
			newItem.Key,
			StatusKeyExists,
			0,
			false)
	}

}

// This sets a single entry into memcache.  If the item's data version id
// (aka CAS) is nonzero, the set operation can only succeed if the item
// exists in memcache and has a same data version id.
func (c *MockClient) Set(item *Item) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.setHelper(item)
}

// Batch version of the Set method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering).
func (c *MockClient) SetMulti(items []*Item) []MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := make([]MutateResponse, len(items))
	for i, item := range items {
		res[i] = c.setHelper(item)
	}
	return res
}

func (c *MockClient) SetSentinels(items []*Item) []MutateResponse {
	// TODO(patrick): Support state mocking
	return c.SetMulti(items)
}

func (c *MockClient) addHelper(item *Item) MutateResponse {
	c.version++

	newItem := &Item{
		Key:           item.Key,
		Value:         item.Value,
		Flags:         item.Flags,
		Expiration:    item.Expiration,
		DataVersionId: c.version,
	}

	if _, ok := c.data[newItem.Key]; !ok {
		c.data[newItem.Key] = newItem
		return NewMutateResponse(
			newItem.Key,
			StatusNoError,
			newItem.DataVersionId,
			false)
	} else {
		return NewMutateResponse(
			newItem.Key,
			StatusKeyExists,
			0,
			false)
	}
}

// This adds a single entry into memcache.  Note: Add will fail if the
// item already exist in memcache.
func (c *MockClient) Add(item *Item) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.addHelper(item)
}

// Batch version of the Add method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering).
func (c *MockClient) AddMulti(items []*Item) []MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	res := make([]MutateResponse, len(items))
	for i, item := range items {
		res[i] = c.addHelper(item)
	}
	return res
}

// This replaces a single entry in memcache.  Note: Replace will fail if
// the does not exist in memcache.
func (c *MockClient) Replace(item *Item) MutateResponse {
	return NewMutateErrorResponse(
		item.Key,
		errors.Newf("Replace not implemented"))
}

// This deletes a single entry from memcache.
func (c *MockClient) Delete(key string) MutateResponse {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, ok := c.data[key]
	if !ok {
		return NewMutateResponse(
			key,
			StatusKeyNotFound,
			0,
			false)
	}

	delete(c.data, key)

	return NewMutateResponse(
		key,
		StatusNoError,
		0,
		false)
}

// Batch version of the Delete method.  Note that the response entries
// ordering is undefined (i.e., may not match the input ordering)
func (c *MockClient) DeleteMulti(keys []string) []MutateResponse {
	res := make([]MutateResponse, len(keys))
	for i, key := range keys {
		res[i] = c.Delete(key)
	}
	return res
}

// This appends the value bytes to the end of an existing entry.  Note that
// this does not allow you to extend past the item limit.
func (c *MockClient) Append(key string, value []byte) MutateResponse {
	return NewMutateErrorResponse(key, errors.Newf("Append not implemented"))
}

// This prepends the value bytes to the end of an existing entry.  Note that
// this does not allow you to extend past the item limit.
func (c *MockClient) Prepend(key string, value []byte) MutateResponse {
	return NewMutateErrorResponse(key, errors.Newf("Prepend not implemented"))
}

// This increments the key's counter by delta.  If the counter does not
// exist, one of two things may happen:
// 1. If the expiration value is all one-bits (0xffffffff), the operation
// will fail with StatusNotFound.
// 2. For all other expiration values, the operation will succeed by
// seeding the value for this key with the provided initValue to expire
// with the provided expiration time. The flags will be set to zero.
//
// NOTE:
// 1. If you want to set the value of the counter with add/set/replace,
// the objects data must be the ascii representation of the value and
// not the byte values of a 64 bit integer.
// 2. Incrementing the counter may cause the counter to wrap.
func (c *MockClient) Increment(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return NewCountErrorResponse(key, errors.Newf("Increment not implemented"))
}

// This decrements the key's counter by delta.  If the counter does not
// exist, one of two things may happen:
// 1. If the expiration value is all one-bits (0xffffffff), the operation
// will fail with StatusNotFound.
// 2. For all other expiration values, the operation will succeed by
// seeding the value for this key with the provided initValue to expire
// with the provided expiration time. The flags will be set to zero.
//
// NOTE:
// 1. If you want to set the value of the counter with add/set/replace,
// the objects data must be the ascii representation of the value and
// not the byte values of a 64 bit integer.
// 2. Decrementing a counter will never result in a "negative value" (or
// cause the counter to "wrap"). instead the counter is set to 0.
func (c *MockClient) Decrement(
	key string,
	delta uint64,
	initValue uint64,
	expiration uint32) CountResponse {

	return NewCountErrorResponse(key, errors.Newf("Decrement not implemented"))
}

// This invalidates all existing cache items after expiration number of
// seconds.
func (c *MockClient) Flush(expiration uint32) Response {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// TODO(patrick): Use expiration argument
	c.data = make(map[string]*Item)
	return NewResponse(StatusNoError)
}

// This requests the server statistics. When the key is an empty string,
// the server will respond with a "default" set of statistics information.
func (c *MockClient) Stat(statsKey string) StatResponse {
	return NewStatErrorResponse(errors.Newf("Stat not implemented"), nil)
}

// This returns the server's version string.
func (c *MockClient) Version() VersionResponse {
	return NewVersionResponse(StatusNoError, map[int]string{0: "MockSever"})
}

// This set the verbosity level of the server.
func (c *MockClient) Verbosity(verbosity uint32) Response {
	return NewResponse(StatusNoError)
}
