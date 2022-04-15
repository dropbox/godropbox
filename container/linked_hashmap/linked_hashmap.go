package linked_hashmap

import "container/list"

// LinkedHashmap provides a basic linked hashmap container. It maintains insertion order
// via a linked list. This gives O(1) runtime to push a new element to the back of the list
// and O(1) to remove the first element in the list. The additional hashmap allows O(1) lookup or
// removal of any element by key.
// Not threadsafe.
type LinkedHashmap struct {
	linkedList *list.List
	hashMap    map[string]*list.Element
}

func NewLinkedHashmap(sizeEst int) *LinkedHashmap {
	return &LinkedHashmap{
		linkedList: list.New(),
		hashMap:    make(map[string]*list.Element, sizeEst),
	}
}

// We store both the key and value in the linked list so we have the key for PopFront/Back.
type kv struct {
	key   string
	value interface{}
}

// Returns the first element, or nil if the list is empty.
func (l *LinkedHashmap) Front() interface{} {
	head := l.linkedList.Front()
	if head == nil {
		return nil
	}
	keyVal := head.Value.(*kv)
	return keyVal.value
}

// Note: Panics if remove is called with a key not in the hashmap.
func (l *LinkedHashmap) Remove(key string) {
	listElem := l.hashMap[key]
	delete(l.hashMap, key)
	l.linkedList.Remove(listElem)
}

func (l *LinkedHashmap) PushBack(key string, val interface{}) {
	elem := l.linkedList.PushBack(&kv{key: key, value: val})
	l.hashMap[key] = elem
}

func (l *LinkedHashmap) PushFront(key string, val interface{}) {
	elem := l.linkedList.PushFront(&kv{key: key, value: val})
	l.hashMap[key] = elem
}

func (l *LinkedHashmap) PopFront() (key string, val interface{}) {
	elem := l.linkedList.Front()
	keyVal := elem.Value.(*kv)
	l.Remove(keyVal.key)
	return keyVal.key, keyVal.value
}

func (l *LinkedHashmap) PopBack() (key string, val interface{}) {
	elem := l.linkedList.Back()
	keyVal := elem.Value.(*kv)
	l.Remove(keyVal.key)
	return keyVal.key, keyVal.value
}

// Panics if MoveToFront is called with a key not in the hashmap.
func (l *LinkedHashmap) MoveToFront(key string) {
	elem := l.hashMap[key]
	l.linkedList.MoveToFront(elem)
}

func (l *LinkedHashmap) Len() int {
	return len(l.hashMap)
}

// Same semantics as the golang map -- returns the elements + true if the key exists
// in the map and nil, false otherise.
func (l *LinkedHashmap) Get(key string) (interface{}, bool) {
	elem, ok := l.hashMap[key]
	if !ok {
		return nil, false
	}
	keyVal := elem.Value.(*kv)
	return keyVal.value, true
}
