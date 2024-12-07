package main

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
)

// Node represents a server or node in the distributed system
type Node struct {
	ID       string
	Replicas int
}

// ConsistentHash manages the distribution of keys across nodes
type ConsistentHash struct {
	circle        map[uint32]string
	sortedKeys    []uint32
	nodes         map[string]*Node
	replicaFactor int
	mutex         sync.RWMutex
}

// New creates a new ConsistentHash instance
func New(replicaFactor int) *ConsistentHash {
	return &ConsistentHash{
		circle:        make(map[uint32]string),
		nodes:         make(map[string]*Node),
		replicaFactor: replicaFactor,
	}
}

// hashKey generates a 32-bit hash for a given key
func (ch *ConsistentHash) hashKey(key string) uint32 {
	hash := md5.Sum([]byte(key))
	return uint32(hash[0]) | uint32(hash[1])<<8 | uint32(hash[2])<<16 | uint32(hash[3])<<24
}

// AddNode adds a new node to the consistent hash ring
func (ch *ConsistentHash) AddNode(nodeID string) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	if _, exists := ch.nodes[nodeID]; exists {
		return
	}

	node := &Node{
		ID:       nodeID,
		Replicas: ch.replicaFactor,
	}
	ch.nodes[nodeID] = node

	// Add virtual nodes for better distribution
	for i := 0; i < ch.replicaFactor; i++ {
		virtualNodeKey := fmt.Sprintf("%s:%d", nodeID, i)
		hash := ch.hashKey(virtualNodeKey)
		ch.circle[hash] = nodeID
		ch.sortedKeys = append(ch.sortedKeys, hash)
	}

	sort.Slice(ch.sortedKeys, func(i, j int) bool {
		return ch.sortedKeys[i] < ch.sortedKeys[j]
	})
}

// RemoveNode removes a node from the consistent hash ring
func (ch *ConsistentHash) RemoveNode(nodeID string) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()

	if _, exists := ch.nodes[nodeID]; !exists {
		return
	}

	delete(ch.nodes, nodeID)

	var newSortedKeys []uint32
	for hash, node := range ch.circle {
		if node == nodeID {
			delete(ch.circle, hash)
		} else {
			newSortedKeys = append(newSortedKeys, hash)
		}
	}

	ch.sortedKeys = newSortedKeys
	sort.Slice(ch.sortedKeys, func(i, j int) bool {
		return ch.sortedKeys[i] < ch.sortedKeys[j]
	})
}

// GetNode finds the appropriate node for a given key
func (ch *ConsistentHash) GetNode(key string) string {
	ch.mutex.RLock()
	defer ch.mutex.RUnlock()

	if len(ch.circle) == 0 {
		return ""
	}

	hash := ch.hashKey(key)
	idx := sort.Search(len(ch.sortedKeys), func(i int) bool {
		return ch.sortedKeys[i] >= hash
	})

	// Wrap around if we reach the end
	if idx == len(ch.sortedKeys) {
		idx = 0
	}

	return ch.circle[ch.sortedKeys[idx]]
}

// PrintDistribution prints the distribution of nodes in the hash ring
func (ch *ConsistentHash) PrintDistribution() {
	ch.mutex.RLock()
	defer ch.mutex.RUnlock()

	fmt.Println("Consistent Hash Ring Distribution:")
	for hash, node := range ch.circle {
		fmt.Printf("Hash: %d -> Node: %s\n", hash, node)
	}
}

func main() {
	ch := New(3) // 3 replicas per node

	// Add nodes
	ch.AddNode("server1")
	ch.AddNode("server2")
	ch.AddNode("server3")

	// Print node distribution
	ch.PrintDistribution()

	// Get node for different keys
	keys := []string{"user123", "product456", "order789"}
	for _, key := range keys {
		node := ch.GetNode(key)
		fmt.Printf("Key: %s -> Node: %s\n", key, node)
	}

	// Remove a node
	ch.RemoveNode("server2")
	fmt.Println("\nAfter removing server2:")
	ch.PrintDistribution()
}
