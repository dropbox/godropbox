package caching

type LocalMapStorage struct {
}

type localMapStorage struct {
	keyStringFunc       ToStringFunc
	itemToKeyStringFunc ToStringFunc

	keyVal map[string]interface{}
}

func (s *localMapStorage) get(key interface{}) (interface{}, error) {
	if item, inMap := s.keyVal[s.keyStringFunc(key)]; inMap {
		return item, nil
	}
	return nil, nil
}

func (s *localMapStorage) set(item interface{}) error {
	s.keyVal[s.itemToKeyStringFunc(item)] = item
	return nil
}

func (s *localMapStorage) del(key interface{}) error {
	delete(s.keyVal, s.keyStringFunc(key))
	return nil
}

func (s *localMapStorage) flush() error {
	s.keyVal = make(map[string]interface{})
	return nil
}

func (s *localMapStorage) size() int {
	return len(s.keyVal)
}

func newLocalMapStorage(
	name string,
	KeyStringFunc ToStringFunc,
	ItemToKeyStringFunc ToStringFunc) (*localMapStorage, Storage) {

	storage := &localMapStorage{
		keyStringFunc:       KeyStringFunc,
		itemToKeyStringFunc: ItemToKeyStringFunc,
		keyVal:              make(map[string]interface{}),
	}

	options := GenericStorageOptions{
		GetFunc:   storage.get,
		SetFunc:   storage.set,
		DelFunc:   storage.del,
		FlushFunc: storage.flush,
	}

	return storage, NewGenericStorage(name, options)
}

// This returns a local non-persistent storage which uses map[string]interface{}
// as its underlying storage.
func NewLocalMapStorage(
	name string,
	KeyStringFunc ToStringFunc,
	ItemToKeyStringFunc ToStringFunc) Storage {

	_, storage := newLocalMapStorage(name, KeyStringFunc, ItemToKeyStringFunc)
	return storage
}
