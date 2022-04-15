package concurrent

type CacheType string

type CacheStore interface {
	// Retrieves a single item from the cache type of CacheStore and whether it exists
	Get(cacheType CacheType, key string) (interface{}, bool)
	// Sets a single item in the cache type of CacheStore
	Set(cacheType CacheType, key string, value interface{}) error
}
