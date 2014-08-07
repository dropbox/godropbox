package singleton

import (
	"sync"
)

type SingletonInitFunc func() (interface{}, error)

// Interface for accessing singleton objects.
//
// Example use:
// var configSelectorSingleton = NewSingleton(init)
// func configSelector() (configSelector, error) {
//     s, err := configSelectorSingleton.Get()
//     if err != nil {
//         return nil, err
//     }
//     return s.(configSelector), nil
// }
type Singleton interface {
	// Return the encapsulated singleton
	Get() (interface{}, error)
}

// Call to create a new singleton that is instantiated with the given init function.
// init is not called until the first invocation of Get().  If init errors, it will be called again
// on the next invocation of Get().
func NewSingleton(init SingletonInitFunc) Singleton {
	return &singletonImpl{init: init}
}

type singletonImpl struct {
	sync.Mutex

	// The actual singleton object
	data interface{}
	// Constructor for the singleton object
	init SingletonInitFunc
	// True if init was run without error
	initialized bool
}

func (s *singletonImpl) Get() (interface{}, error) {
	// Don't lock in the common case
	if s.initialized {
		return s.data, nil
	}

	s.Lock()
	defer s.Unlock()

	if s.initialized {
		return s.data, nil
	}

	var err error
	s.data, err = s.init()
	if err != nil {
		return nil, err
	}

	s.initialized = true
	return s.data, nil
}
