package util

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	maxLevel    = 16
	probability = 0.5
)
 
// SkipListNode represents a node in the skip list
type SkipListNode struct {
	key     []byte
	value   []byte
	forward []*SkipListNode
}

// SkipList is a probabilistic data structure that allows O(log n) search complexity
type SkipList struct {
	header *SkipListNode
	level  int
	length int
	mutex  sync.RWMutex
	rand   *rand.Rand
}

// NewSkipList creates a new skip list
func NewSkipList() *SkipList {
	return &SkipList{
		header: &SkipListNode{
			forward: make([]*SkipListNode, maxLevel),
		},
		level:  0,
		length: 0,
		rand:   rand.New(rand.NewSource(0)),
	}
}

// randomLevel generates a random level for a new node
func (sl *SkipList) randomLevel() int {
	level := 0
	for level < maxLevel-1 && sl.rand.Float64() < probability {
		level++
	}
	return level
}

// Insert adds or updates a key-value pair in the skip list
func (sl *SkipList) Insert(key, value []byte) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	update := make([]*SkipListNode, maxLevel)
	current := sl.header

	// Find the position to insert
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Move to the next node
	current = current.forward[0]

	// If key already exists, update the value
	if current != nil && bytes.Equal(current.key, key) {
		current.value = value
		return
	}

	// Generate a random level for the new node
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	// Create new node
	newNode := &SkipListNode{
		key:     key,
		value:   value,
		forward: make([]*SkipListNode, newLevel+1),
	}

	// Insert the new node
	for i := 0; i <= newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.length++
}

// Search finds a value by key
func (sl *SkipList) Search(key []byte) ([]byte, bool) {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()

	current := sl.header

	// Traverse from the highest level to the lowest
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	// Move to the next node
	current = current.forward[0]

	// Check if we found the key
	if current != nil && bytes.Equal(current.key, key) {
		return current.value, true
	}

	return nil, false
}

// Delete removes a key from the skip list
func (sl *SkipList) Delete(key []byte) bool {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	update := make([]*SkipListNode, maxLevel)
	current := sl.header

	// Find the node to delete
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	// If key doesn't exist
	if current == nil || !bytes.Equal(current.key, key) {
		return false
	}

	// Remove the node
	for i := 0; i <= sl.level; i++ {
		if update[i].forward[i] != current {
			break
		}
		update[i].forward[i] = current.forward[i]
	}

	// Update the level
	for sl.level > 0 && sl.header.forward[sl.level] == nil {
		sl.level--
	}

	sl.length--
	return true
}

// Len returns the number of elements in the skip list
func (sl *SkipList) Len() int {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	return sl.length
}

// Iterator represents an iterator over the skip list
type Iterator struct {
	current *SkipListNode
	sl      *SkipList
}

// NewIterator creates a new iterator starting from the beginning
func (sl *SkipList) NewIterator() *Iterator {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()

	return &Iterator{
		current: sl.header,
		sl:      sl,
	}
}

// Next moves the iterator to the next element
func (it *Iterator) Next() bool {
	it.sl.mutex.RLock()
	defer it.sl.mutex.RUnlock()

	if it.current == nil {
		return false
	}

	it.current = it.current.forward[0]
	return it.current != nil
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.key
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.value
}

// Valid returns true if the iterator is pointing to a valid element
func (it *Iterator) Valid() bool {
	return it.current != nil && it.current != it.sl.header
}

// Seek moves the iterator to the first key >= target
func (it *Iterator) Seek(target []byte) {
	it.sl.mutex.RLock()
	defer it.sl.mutex.RUnlock()

	current := it.sl.header

	// Find the position
	for i := it.sl.level; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, target) < 0 {
			current = current.forward[i]
		}
	}

	it.current = current
}

// GetAll returns all key-value pairs in sorted order
func (sl *SkipList) GetAll() []struct {
	Key   []byte
	Value []byte
} {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()

	result := make([]struct {
		Key   []byte
		Value []byte
	}, 0, sl.length)

	current := sl.header.forward[0]
	for current != nil {
		result = append(result, struct {
			Key   []byte
			Value []byte
		}{
			Key:   current.key,
			Value: current.value,
		})
		current = current.forward[0]
	}

	return result
}
