package caching

// A storage implementation where a cache is layered on top of a storage.  This
// implementation DOES NOT ensure data consistent between cache and storage
// when setting items.
//
// NOTE: Dropbox internally uses a different caching implementation which
// performs two-phase cache invalidation;  this ensures the cached data is
// consistent with the stored data.
type CacheOnStorage struct {
	cache   Storage
	storage Storage
}

// This returns a CacheOnStorage, which adds a cache layer on top of the
// storage.
func NewCacheOnStorage(
	cache Storage,
	storage Storage) Storage {

	return &CacheOnStorage{
		cache:   cache,
		storage: storage,
	}
}

// See Storage for documentation.
func (s *CacheOnStorage) Get(key interface{}) (interface{}, error) {
	if item, err := s.cache.Get(key); err != nil {
		return nil, err
	} else if item != nil {
		return item, nil
	}

	item, err := s.storage.Get(key)
	if err != nil {
		return nil, err
	}

	if err := s.cache.Set(item); err != nil {
		// XXX: Maybe make this a non error
		return nil, err
	}
	return item, nil
}

// See Storage for documentation.
func (s *CacheOnStorage) GetMulti(
	keys ...interface{}) ([]interface{}, error) {

	results, err := s.cache.GetMulti(keys...)
	if err != nil {
		return nil, err
	}

	indices := make([]int, 0, len(keys))
	uncachedKeys := make([]interface{}, 0, len(keys))
	for i, item := range results {
		if item == nil {
			indices = append(indices, i)
			uncachedKeys = append(uncachedKeys, keys[i])
		}
	}

	uncachedItems, err := s.storage.GetMulti(uncachedKeys...)
	if err != nil {
		return nil, err
	}

	foundItems := make([]interface{}, 0, len(uncachedItems))
	for _, item := range uncachedItems {
		if item != nil {
			foundItems = append(foundItems, item)
		}
	}

	if err := s.cache.SetMulti(foundItems...); err != nil {
		// XXX: Maybe make this a non error
		return nil, err
	}

	for i, index := range indices {
		results[index] = uncachedItems[i]
	}

	return results, nil
}

// See Storage for documentation.
func (s *CacheOnStorage) Set(item interface{}) error {
	if err := s.storage.Set(item); err != nil {
		return err
	}

	return s.cache.Set(item)
}

// See Storage for documentation.
func (s *CacheOnStorage) SetMulti(items ...interface{}) error {
	if err := s.storage.SetMulti(items...); err != nil {
		return err
	}

	return s.cache.SetMulti(items...)
}

// See Storage for documentation.
func (s *CacheOnStorage) Delete(key interface{}) error {
	if err := s.storage.Delete(key); err != nil {
		return err
	}

	return s.cache.Delete(key)
}

// See Storage for documentation.
func (s *CacheOnStorage) DeleteMulti(keys ...interface{}) error {
	if err := s.storage.DeleteMulti(keys...); err != nil {
		return err
	}

	return s.cache.DeleteMulti(keys...)
}

// See Storage for documentation.
func (s *CacheOnStorage) Flush() error {
	if err := s.storage.Flush(); err != nil {
		return err
	}

	return s.cache.Flush()
}
