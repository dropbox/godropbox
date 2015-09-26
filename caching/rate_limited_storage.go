package caching

// A storage implementation which limits the maximum number of concurrent
// operations.
type RateLimitedStorage struct {
	semaphore chan bool
	storage   Storage
}

// This returns a RateLimitedStorage.  This is useful for cases where
// high concurrent load may degrade the underlying storage's performance.
// NOTE: when maxConcurrency is non-positive, the original storage is returned
// (i.e., the storage is not rate limited).
func NewRateLimitedStorage(storage Storage, maxConcurrency int) Storage {
	if maxConcurrency < 1 {
		return storage
	}

	return &RateLimitedStorage{
		semaphore: make(chan bool, maxConcurrency),
		storage:   storage,
	}
}

func (s *RateLimitedStorage) wait() {
	s.semaphore <- true
}

func (s *RateLimitedStorage) signal() {
	select {
	case <-s.semaphore:
	default:
	}
}

// See Storage for documentation.
func (s *RateLimitedStorage) Get(key interface{}) (interface{}, error) {
	s.wait()
	defer s.signal()

	return s.storage.Get(key)
}

// See Storage for documentation.
func (s *RateLimitedStorage) GetMulti(
	keys ...interface{}) ([]interface{}, error) {

	s.wait()
	defer s.signal()

	return s.storage.GetMulti(keys)
}

// See Storage for documentation.
func (s *RateLimitedStorage) Set(item interface{}) error {
	s.wait()
	defer s.signal()

	return s.storage.Set(item)
}

// See Storage for documentation.
func (s *RateLimitedStorage) SetMulti(items ...interface{}) error {
	s.wait()
	defer s.signal()

	return s.storage.SetMulti(items)
}

// See Storage for documentation.
func (s *RateLimitedStorage) Delete(key interface{}) error {
	s.wait()
	defer s.signal()

	return s.storage.Delete(key)
}

// See Storage for documentation.
func (s *RateLimitedStorage) DeleteMulti(keys ...interface{}) error {
	s.wait()
	defer s.signal()

	return s.storage.DeleteMulti(keys)
}

// See Storage for documentation.
func (s *RateLimitedStorage) Flush() error {
	s.wait()
	defer s.signal()

	return s.storage.Flush()
}
