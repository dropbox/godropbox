package hashring

import (
	"crypto/md5"
	"fmt"
	"sort"
)

type hashKey uint32
type hashKeyOrders []hashKey

func (h hashKeyOrders) Len() int           { return len(h) }
func (h hashKeyOrders) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h hashKeyOrders) Less(i, j int) bool { return h[i] < h[j] }

type HashRing struct {
	ring       map[hashKey]string
	sortedKeys []hashKey
	nodes      []string
}

// This must be kept in sync with Python `class HashRing`
func New(nodes []string) *HashRing {
	hashRing := &HashRing{
		ring:       make(map[hashKey]string),
		sortedKeys: make([]hashKey, 0),
		nodes:      nodes,
	}
	hashRing.generateCircle()
	return hashRing
}

func (h *HashRing) generateCircle() {
	for _, node := range h.nodes {
		for j := 0; j < 40; j++ {
			nodeKey := fmt.Sprintf("%s-%d", node, j)
			bKey := hashDigest(nodeKey)

			for i := 0; i < 3; i++ {
				key := hashVal(bKey[i*4 : i*4+4])
				h.ring[key] = node
				h.sortedKeys = append(h.sortedKeys, key)
			}
		}
	}

	sort.Sort(hashKeyOrders(h.sortedKeys))
}

func (h *HashRing) GetNode(stringKey string) string {
	if len(h.ring) == 0 {
		return ""
	}

	pos := h.getNodePos(stringKey)
	return h.ring[h.sortedKeys[pos]]
}

// Requires len(h.ring) > 0
func (h *HashRing) getNodePos(stringKey string) (pos int) {
	key := genKey(stringKey)

	nodes := h.sortedKeys
	pos = sort.Search(len(nodes), func(i int) bool { return nodes[i] > key })

	if pos == len(nodes) {
		// Wrap the search, should return first node
		return 0
	} else {
		return
	}
}

func (h *HashRing) GetNodes(stringKey string) []string {
	if len(h.ring) == 0 {
		return nil
	}

	pos := h.getNodePos(stringKey)

	returnedValues := make(map[string]bool, len(h.nodes))
	resultSlice := make([]string, 0, len(h.nodes))

	for i := pos; i < pos+len(h.sortedKeys); i++ {
		key := h.sortedKeys[i%len(h.sortedKeys)]
		val := h.ring[key]
		if !returnedValues[val] {
			returnedValues[val] = true
			resultSlice = append(resultSlice, val)
		}
		if len(returnedValues) == len(h.nodes) {
			break
		}
	}

	return resultSlice
}

func genKey(key string) hashKey {
	bKey := hashDigest(key)
	return hashVal(bKey[0:4])
}

func hashVal(bKey []byte) hashKey {
	return ((hashKey(bKey[3]) << 24) |
		(hashKey(bKey[2]) << 16) |
		(hashKey(bKey[1]) << 8) |
		(hashKey(bKey[0])))
}

func hashDigest(key string) [md5.Size]byte {
	return md5.Sum([]byte(key))
}
