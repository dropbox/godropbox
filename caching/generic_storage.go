package caching

import (
	"sync"

	"github.com/dropbox/godropbox/errors"
)

// Options used in GenericStorage construction.
type GenericStorageOptions struct {
	// GenericStorage will call either GetFunc or GetMultiFunc in its
	// Get and GetMulti implementations.  When neither one is available,
	// GenericStorage will return error.
	GetFunc      func(key interface{}) (interface{}, error)
	GetMultiFunc func(keys ...interface{}) ([]interface{}, error)

	// GenericStorage will call either SetFunc or SetMultiFunc in its
	// Set and SetMulti implementations.  When neither one is available,
	// GenericStorage will return error.
	SetFunc      func(item interface{}) error
	SetMultiFunc func(items ...interface{}) error

	// GenericStorage will call either DelFunc or DelMultiFunc in its
	// Del and DelMulti implementations.  When neither one is available,
	// GenericStorage will return error.
	DelFunc      func(key interface{}) error
	DelMultiFunc func(keys ...interface{}) error

	// When ErrorOnFlush is true, GenericStorage will always return error
	// on Flush calls.
	ErrorOnFlush bool

	// GenericStorage will call FlushFunc in its Flush implementation.  When
	// FlushFunc is unavailable (and ErrorOnFlush is false), GenericStorage
	// will do nothing and return nil.
	FlushFunc func() error
}

// A generic storage implementation.  The functionalities are provided by the
// user through GenericStorageOptions.
type GenericStorage struct {
	name string

	rwMutex sync.RWMutex

	get      func(key interface{}) (interface{}, error)
	getMulti func(keys ...interface{}) ([]interface{}, error)
	set      func(item interface{}) error
	setMulti func(items ...interface{}) error
	del      func(key interface{}) error
	delMulti func(keys ...interface{}) error

	errorOnFlush bool
	flush        func() error
}

// This creates a GenericStorage.  See GenericStorageOptions for additional
// information.
func NewGenericStorage(name string, options GenericStorageOptions) Storage {
	return &GenericStorage{
		name:         name,
		get:          options.GetFunc,
		getMulti:     options.GetMultiFunc,
		set:          options.SetFunc,
		setMulti:     options.SetMultiFunc,
		del:          options.DelFunc,
		delMulti:     options.DelMultiFunc,
		errorOnFlush: options.ErrorOnFlush,
		flush:        options.FlushFunc,
	}
}

// See Storage/GenericStorageOptions for documentation.
func (s *GenericStorage) Get(key interface{}) (interface{}, error) {
	if s.get == nil && s.getMulti == nil {
		return nil, errors.Newf(
			"'%s' does not have Get/GetMulti implementation",
			s.name)
	}

	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	if s.get != nil {
		return s.get(key)
	}

	items, err := s.getMulti(key)
	if err != nil {
		return nil, err
	}
	return items[0], nil
}

// See Storage/GenericStorageOptions for documentation.
func (s *GenericStorage) GetMulti(keys ...interface{}) ([]interface{}, error) {
	if s.get == nil && s.getMulti == nil {
		return nil, errors.Newf(
			"'%s' does not have Get/GetMulti implementation",
			s.name)
	}

	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	if s.getMulti != nil {
		return s.getMulti(keys...)
	}

	results := make([]interface{}, len(keys))
	for i, key := range keys {
		item, err := s.get(key)
		if err != nil {
			return nil, err
		}
		results[i] = item
	}
	return results, nil
}

// See Storage/GenericStorageOptions for documentation.
func (s *GenericStorage) Set(item interface{}) error {
	if s.set == nil && s.setMulti == nil {
		return errors.Newf(
			"'%s' does not have Set/SetMulti implementation",
			s.name)
	}

	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	if s.set != nil {
		return s.set(item)
	}
	return s.setMulti(item)
}

// See Storage/GenericStorageOptions for documentation.
func (s *GenericStorage) SetMulti(items ...interface{}) error {
	if s.set == nil && s.setMulti == nil {
		return errors.Newf(
			"'%s' does not have Set/SetMulti implementation",
			s.name)
	}

	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	if s.setMulti != nil {
		return s.setMulti(items...)
	}

	for _, item := range items {
		if err := s.set(item); err != nil {
			return err
		}
	}
	return nil
}

// See Storage/GenericStorageOptions for documentation.
func (s *GenericStorage) Delete(key interface{}) error {
	if s.del == nil && s.delMulti == nil {
		return errors.Newf(
			"'%s' does not have Delete/DeleteMulti implementation",
			s.name)
	}

	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	if s.del != nil {
		return s.del(key)
	}
	return s.delMulti(key)
}

// See Storage/GenericStorageOptions for documentation.
func (s *GenericStorage) DeleteMulti(keys ...interface{}) error {
	if s.del == nil && s.delMulti == nil {
		return errors.Newf(
			"'%s' does not have Delete/DeleteMulti implementation",
			s.name)
	}

	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	if s.delMulti != nil {
		return s.delMulti(keys...)
	}

	for _, key := range keys {
		if err := s.del(key); err != nil {
			return err
		}
	}
	return nil
}

// See Storage/GenericStorageOptions for documentation.
func (s *GenericStorage) Flush() error {
	if s.errorOnFlush {
		return errors.Newf("'%s' does not support Flush", s.name)
	}

	if s.flush == nil {
		return nil
	}

	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	return s.flush()
}
