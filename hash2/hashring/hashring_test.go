package hashring

import (
	"reflect"
	"testing"
)

var (
	benchRing = New([]string{"a", "b", "c", "d", "e", "f", "g"})
	benchKeys = []string{
		"test", "test", "test1", "test2", "test3", "test4", "test5", "aaaa", "bbbb"}
)

func expectNode(t *testing.T, hashRing *HashRing, key string, expectedNode string) {
	node := hashRing.GetNode(key)
	if node != expectedNode {
		t.Error("GetNode(", key, ") expected", expectedNode, "but got", node)
	}
}

func expectNodes(t *testing.T, hashRing *HashRing, key string, expectedNodes []string) {
	nodes := hashRing.GetNodes(key)
	sliceEquality := reflect.DeepEqual(nodes, expectedNodes)
	if !sliceEquality {
		t.Error("GetNodes(", key, ") expected", expectedNodes, "but got", nodes)
	}
}

// These tests must be kept in sync with Python `class HashRingTests`
func TestNew(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	hashRing := New(nodes)

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "c")
	expectNode(t, hashRing, "test4", "c")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")

	expectNodes(t, hashRing, "test", []string{"a", "b", "c"})
	expectNodes(t, hashRing, "test", []string{"a", "b", "c"})
	expectNodes(t, hashRing, "test1", []string{"b", "c", "a"})
	expectNodes(t, hashRing, "test2", []string{"b", "a", "c"})
	expectNodes(t, hashRing, "test3", []string{"c", "a", "b"})
	expectNodes(t, hashRing, "test4", []string{"c", "b", "a"})
	expectNodes(t, hashRing, "test5", []string{"a", "c", "b"})
	expectNodes(t, hashRing, "aaaa", []string{"b", "a", "c"})
	expectNodes(t, hashRing, "bbbb", []string{"a", "b", "c"})
}

func TestNewEmpty(t *testing.T) {
	nodes := []string{}
	hashRing := New(nodes)

	node := hashRing.GetNode("test")
	if node != "" {
		t.Error("GetNode(test) expected (\"\") but got (", node, ")")
	}

	rNodes := hashRing.GetNodes("test")
	if !(len(rNodes) == 0) {
		t.Error("GetNode(test) expected ( [], false ) but got (", rNodes, ")")
	}
}

func TestNewSingle(t *testing.T) {
	nodes := []string{"a"}
	hashRing := New(nodes)

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "a")
	expectNode(t, hashRing, "test2", "a")
	expectNode(t, hashRing, "test3", "a")

	// This triggers the edge case where sortedKey search resulting in not found
	expectNode(t, hashRing, "test14", "a")

	expectNode(t, hashRing, "test15", "a")
	expectNode(t, hashRing, "test16", "a")
	expectNode(t, hashRing, "test17", "a")
	expectNode(t, hashRing, "test18", "a")
	expectNode(t, hashRing, "test19", "a")
	expectNode(t, hashRing, "test20", "a")
}

func TestDuplicateNodes(t *testing.T) {
	nodes := []string{"a", "a", "a", "a", "b"}
	hashRing := New(nodes)

	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test", "a")
	expectNode(t, hashRing, "test1", "b")
	expectNode(t, hashRing, "test2", "b")
	expectNode(t, hashRing, "test3", "a")
	expectNode(t, hashRing, "test4", "b")
	expectNode(t, hashRing, "test5", "a")
	expectNode(t, hashRing, "aaaa", "b")
	expectNode(t, hashRing, "bbbb", "a")
}

func BenchmarkHashes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchRing.GetNodes(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkHashesSingle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchRing.GetNode(benchKeys[i%len(benchKeys)])
	}
}
