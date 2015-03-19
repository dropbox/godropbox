package set

// An unordered collection of unique elements which supports lookups, insertions, deletions,
// iteration, and common binary set operations.  It is not guaranteed to be thread-safe.
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

// Returns a new set which is the union of s1 and s2.  s1 and s2 are unmodified.
func Union(s1 Set, s2 Set) Set {
	s3 := s1.Copy()
	s3.Union(s2)
	return s3
}

// Returns a new set which is the intersect of s1 and s2.  s1 and s2 are
// unmodified.
func Intersect(s1 Set, s2 Set) Set {
	s3 := s1.Copy()
	s3.Intersect(s2)
	return s3
}

// Returns a new set which is the difference between s1 and s2.  s1 and s2 are
// unmodified.
func Subtract(s1 Set, s2 Set) Set {
	s3 := s1.Copy()
	s3.Subtract(s2)
	return s3
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

// Returns a new Set pre-populated with the given items
func NewKeyedSet(keyf func(interface{}) interface{}, items ...interface{}) Set {
	res := keyedSetImpl{
		data:    make(map[interface{}]interface{}),
		keyfunc: keyf,
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
	union(s, s2)
}

func (s setImpl) Intersect(s2 Set) {
	var toRemove []interface{}
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
	subtract(s, s2)
}

func (s setImpl) IsSubset(s2 Set) (isSubset bool) {
	return subset(s, s2)
}

func (s setImpl) IsSuperset(s2 Set) bool {
	return superset(s, s2)
}

func (s setImpl) IsEqual(s2 Set) bool {
	return equal(s, s2)
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

// keyedSetImpl implementation below here

type keyedSetImpl struct {
	data    map[interface{}]interface{}
	keyfunc (func(interface{}) interface{})
}

func (s keyedSetImpl) Len() int {
	return len(s.data)
}

func (s keyedSetImpl) Copy() Set {
	res := NewKeyedSet(s.keyfunc)
	res.Union(s)
	return res
}

func (s keyedSetImpl) Init() {
	s.data = make(map[interface{}]interface{})
}

func (s keyedSetImpl) Contains(v interface{}) bool {
	_, ok := s.data[s.keyfunc(v)]
	return ok
}

func (s keyedSetImpl) Add(v interface{}) {
	s.data[s.keyfunc(v)] = v
}

func (s keyedSetImpl) Remove(v interface{}) bool {
	key := s.keyfunc(v)
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok

}

func (s keyedSetImpl) Do(f func(interface{})) {
	for _, v := range s.data {
		f(v)
	}
}

func (s keyedSetImpl) DoWhile(f func(interface{}) bool) {
	for _, v := range s.data {
		if !f(v) {
			break
		}
	}
}

func (s keyedSetImpl) Iter() <-chan interface{} {
	iter := make(chan interface{})
	go func() {
		for _, v := range s.data {
			iter <- v
		}
		close(iter)
	}()
	return iter
}

func (s keyedSetImpl) Union(s2 Set) {
	union(s, s2)
}

func (s keyedSetImpl) Intersect(s2 Set) {
	var toRemove []interface{}
	for _, v := range s.data {
		if !s2.Contains(v) {
			toRemove = append(toRemove, v)
		}
	}

	for _, v := range toRemove {
		s.Remove(v)
	}
}

func (s keyedSetImpl) Subtract(s2 Set) {
	subtract(s, s2)
}

func (s keyedSetImpl) IsSubset(s2 Set) (isSubset bool) {
	return subset(s, s2)
}

func (s keyedSetImpl) IsSuperset(s2 Set) bool {
	return superset(s, s2)
}

func (s keyedSetImpl) IsEqual(s2 Set) bool {
	return equal(s, s2)
}

func (s keyedSetImpl) RemoveIf(f func(interface{}) bool) {
	var toRemove []interface{}
	for _, item := range s.data {
		if f(item) {
			toRemove = append(toRemove, item)
		}
	}

	for _, item := range toRemove {
		s.Remove(item)
	}
}

// Common functions between the two implementations, since go
// does not allow for any inheritance.

func equal(s Set, s2 Set) bool {
	if s.Len() != s2.Len() {
		return false
	}

	return s.IsSubset(s2)
}

func superset(s Set, s2 Set) bool {
	return s2.IsSubset(s)
}

func subset(s Set, s2 Set) (isSubset bool) {
	isSubset = true
	s.DoWhile(func(item interface{}) bool {
		if !s2.Contains(item) {
			isSubset = false
		}
		return isSubset
	})
	return
}

func subtract(s Set, s2 Set) {
	s2.Do(func(item interface{}) { s.Remove(item) })
}

func union(s Set, s2 Set) {
	s2.Do(func(item interface{}) { s.Add(item) })
}
