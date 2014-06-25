// A utility library for building caching layers.
package caching

// A generic key value storage interface.  The storage may be persistent
// (e.g., a database) or volatile (e.g., cache).  All Storage implementations
// must be thread safe.
type Storage interface {
	// This retrieves a single value from the storage.
	Get(key interface{}) (interface{}, error)

	// This retrieves multiple values from the storage.  The items are returned
	// in the same order as the input keys.
	GetMulti(keys ...interface{}) ([]interface{}, error)

	// This stores a single item into the storage.
	Set(item interface{}) error

	// This stores multiple items into the storage.
	SetMulti(items ...interface{}) error

	// This removes a single item from the storage.
	Delete(key interface{}) error

	// This removes multiple items from the storage.
	DeleteMulti(keys ...interface{}) error

	// This wipes all items from the storage.
	Flush() error
}

type ToStringFunc (func(key interface{}) string)
