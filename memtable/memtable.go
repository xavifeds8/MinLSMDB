package memtable

import (
	"fmt"
	"sync"
 
	"minilsm/util"
)

// Tombstone is a special value indicating a deleted key
var Tombstone = []byte{0xFF, 0xFF, 0xFF, 0xFF}

// Entry represents a key-value pair with metadata
type Entry struct {
	Key   []byte
	Value []byte
}

// MemTable is an in-memory sorted data structure
type MemTable struct {
	data    *util.SkipList
	size    int64
	maxSize int64
	mutex   sync.RWMutex
}

// New creates a new MemTable with the specified maximum size in bytes
func New(maxSize int64) *MemTable {
	return &MemTable{
		data:    util.NewSkipList(),
		size:    0,
		maxSize: maxSize,
	}
}

// Put inserts or updates a key-value pair
func (mt *MemTable) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	// Check if key already exists to calculate size delta
	oldValue, exists := mt.data.Search(key)
	
	// Calculate size change
	var sizeDelta int64
	if exists {
		// Replacing existing value
		sizeDelta = int64(len(value) - len(oldValue))
	} else {
		// New entry: key + value + overhead (pointers, etc.)
		sizeDelta = int64(len(key) + len(value) + 32) // 32 bytes overhead estimate
	}

	// Insert into skip list
	mt.data.Insert(key, value)
	mt.size += sizeDelta

	return nil
}

// Get retrieves a value by key
// Returns (value, found, error)
// If the key is deleted (tombstone), returns (nil, true, nil)
func (mt *MemTable) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, fmt.Errorf("key cannot be empty")
	}

	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	value, found := mt.data.Search(key)
	if !found {
		return nil, false, nil
	}

	// Check if it's a tombstone (deleted key)
	if isTombstone(value) {
		return nil, true, nil
	}

	return value, true, nil
}

// Delete marks a key as deleted by inserting a tombstone
func (mt *MemTable) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	// Check if key exists
	oldValue, exists := mt.data.Search(key)
	
	var sizeDelta int64
	if exists {
		// Replace with tombstone
		sizeDelta = int64(len(Tombstone) - len(oldValue))
	} else {
		// New tombstone entry
		sizeDelta = int64(len(key) + len(Tombstone) + 32)
	}

	// Insert tombstone
	mt.data.Insert(key, Tombstone)
	mt.size += sizeDelta

	return nil
}

// Size returns the current size of the MemTable in bytes
func (mt *MemTable) Size() int64 {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	return mt.size
}

// MaxSize returns the maximum size threshold
func (mt *MemTable) MaxSize() int64 {
	return mt.maxSize
}

// Len returns the number of entries in the MemTable
func (mt *MemTable) Len() int {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	return mt.data.Len()
}

// IsFull returns true if the MemTable has reached its size threshold
func (mt *MemTable) IsFull() bool {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()
	return mt.size >= mt.maxSize
}

// GetAll returns all entries in sorted order
// This is used when flushing to SSTable
func (mt *MemTable) GetAll() []Entry {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	all := mt.data.GetAll()
	entries := make([]Entry, 0, len(all))

	for _, item := range all {
		entries = append(entries, Entry{
			Key:   item.Key,
			Value: item.Value,
		})
	}

	return entries
}

// NewIterator creates a new iterator for the MemTable
func (mt *MemTable) NewIterator() *Iterator {
	mt.mutex.RLock()
	defer mt.mutex.RUnlock()

	return &Iterator{
		skipListIter: mt.data.NewIterator(),
		mt:           mt,
	}
}

// Iterator provides sequential access to MemTable entries
type Iterator struct {
	skipListIter *util.Iterator
	mt           *MemTable
}

// Next moves to the next entry
func (it *Iterator) Next() bool {
	return it.skipListIter.Next()
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	return it.skipListIter.Key()
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	return it.skipListIter.Value()
}

// Valid returns true if the iterator is at a valid position
func (it *Iterator) Valid() bool {
	return it.skipListIter.Valid()
}

// Seek positions the iterator at the first key >= target
func (it *Iterator) Seek(target []byte) {
	it.skipListIter.Seek(target)
}

// isTombstone checks if a value is a tombstone marker
func isTombstone(value []byte) bool {
	if len(value) != len(Tombstone) {
		return false
	}
	for i := range value {
		if value[i] != Tombstone[i] {
			return false
		}
	}
	return true
}

// Clear removes all entries from the MemTable
// This is typically called after flushing to SSTable
func (mt *MemTable) Clear() {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()

	mt.data = util.NewSkipList()
	mt.size = 0
}
