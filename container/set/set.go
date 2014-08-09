package set

type Set interface {
	Copy() Set

	Len() int

	Contains(v interface{}) bool
	Add(v interface{})
	Remove(v interface{}) bool

	Do(f func(interface{}))
	DoWhile(f func(interface{}) bool)
	Iter() <-chan interface{}
	Union(s Set)
	Intersect(s Set)
	Subtract(s Set)
	Init()
	IsSubset(s Set) bool
	IsSuperset(s Set) bool
	IsEqual(s Set) bool
	RemoveIf(f func(interface{}) bool)
}

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
	for key := range s.data {
		if !s2.Contains(key) {
			s.Remove(key)
		}
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
	for el := range s.Iter() {
		if f(el) {
			s.Remove(el)
		}
	}
}
