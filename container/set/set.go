package set

// An unordered collection of unique elements which supports lookups, insertions, deletions,
// iteration, and common binary set operations.  It is not guaranteed to be thred-safe.
type Set interface {
	// Returns a new Set that contains exactly the same elements as this set.
	Copy() Set

	// Returns the cardinality of this set.
	Len() int

	// Returns true if and only if this set contains v (according to Go equality rules).
	Contains(v interface{}) bool
	// Inserts v into this set.
	Add(v interface{})
	// Removes v from this set, if it is present.  Returns true if and only if v was present.
	Remove(v interface{}) bool

	// Executes f(v) for every element v in this set.  If f mutates this set, behavior is undefined.
	Do(f func(interface{}))
	// Executes f(v) once for every element v in the set, aborting if f ever returns false. If f
	// mutates this set, behavior is undefined.
	DoWhile(f func(interface{}) bool)
	// Returns a channel from which each element in the set can be read exactly once.  If this set
	// is mutated before the channel is emptied, the exact data read from the channel is undefined.
	Iter() <-chan interface{}

	// Adds every element in s into this set.
	Union(s Set)
	// Removes every element not in s from this set.
	Intersect(s Set)
	// Removes every element in s from this set.
	Subtract(s Set)
	// Removes all elements from the set.
	Init()
	// Returns true if and only if all elements in this set are elements in s.
	IsSubset(s Set) bool
	// Returns true if and only if all elements in s are elements in this set.
	IsSuperset(s Set) bool
	// Returns true if and only if this set and s contain exactly the same elements.
	IsEqual(s Set) bool
	// Removes all elements v from this set that satisfy f(v) == true.
	RemoveIf(f func(interface{}) bool)
}

// Returns a new Set pre-populated with the given items
func NewSet(items ...interface{}) Set {
	res := setImpl{
		data: make(map[interface{}]struct{}),
	}
	for _, item := range items {
		res.Add(item)
	}
	return res
}

type setImpl struct {
	data map[interface{}]struct{}
}

func (s setImpl) Len() int {
	return len(s.data)
}

func (s setImpl) Copy() Set {
	res := NewSet()
	res.Union(s)
	return res
}

func (s setImpl) Init() {
	s.data = make(map[interface{}]struct{})
}

func (s setImpl) Contains(v interface{}) bool {
	_, ok := s.data[v]
	return ok
}

func (s setImpl) Add(v interface{}) {
	s.data[v] = struct{}{}
}

func (s setImpl) Remove(v interface{}) bool {
	_, ok := s.data[v]
	if ok {
		delete(s.data, v)
	}
	return ok
}

func (s setImpl) Do(f func(interface{})) {
	for key := range s.data {
		f(key)
	}
}

func (s setImpl) DoWhile(f func(interface{}) bool) {
	for key := range s.data {
		if !f(key) {
			break
		}
	}
}

func (s setImpl) Iter() <-chan interface{} {
	iter := make(chan interface{})
	go func() {
		for key := range s.data {
			iter <- key
		}
		close(iter)
	}()
	return iter
}

func (s setImpl) Union(s2 Set) {
	s2.Do(func(item interface{}) { s.Add(item) })
}

func (s setImpl) Intersect(s2 Set) {
	var toRemove []interface{} = nil
	for key := range s.data {
		if !s2.Contains(key) {
			toRemove = append(toRemove, key)
		}
	}

	for _, key := range toRemove {
		s.Remove(key)
	}
}

func (s setImpl) Subtract(s2 Set) {
	s2.Do(func(item interface{}) { s.Remove(item) })
}

func (s setImpl) IsSubset(s2 Set) (isSubset bool) {
	isSubset = true
	s.DoWhile(func(item interface{}) bool {
		if !s2.Contains(item) {
			isSubset = false
		}
		return isSubset
	})
	return
}

func (s setImpl) IsSuperset(s2 Set) bool {
	return s2.IsSubset(s)
}

func (s setImpl) IsEqual(s2 Set) bool {
	if s.Len() != s2.Len() {
		return false
	}

	return s.IsSubset(s2)
}

func (s setImpl) RemoveIf(f func(interface{}) bool) {
	var toRemove []interface{}
	for item := range s.data {
		if f(item) {
			toRemove = append(toRemove, item)
		}
	}

	for _, item := range toRemove {
		s.Remove(item)
	}
}
