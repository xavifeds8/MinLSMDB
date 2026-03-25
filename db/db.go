package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"minilsm/memtable"
	"minilsm/sstable"
	"minilsm/wal"
)

// DB represents the main database instance
type DB struct {
	path      string
	wal       *wal.WAL
	memtable  *memtable.MemTable
	sstables  []*sstable.SSTable
	mutex     sync.RWMutex
	closed    bool
	flushChan chan struct{}
}

// Options contains configuration for the database
type Options struct {
	MemTableSize int64 // Size threshold for MemTable flush (default: 4MB)
}

// DefaultOptions returns default database options
func DefaultOptions() Options {
	return Options{
		MemTableSize: 4 * 1024 * 1024, // 4MB
	}
}

// Open opens or creates a database at the specified path
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, DefaultOptions())
}

// OpenWithOptions opens a database with custom options
func OpenWithOptions(path string, opts Options) (*DB, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Open WAL
	w, err := wal.New(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	// Create MemTable
	mt := memtable.New(opts.MemTableSize)

	// Replay WAL to rebuild MemTable
	entries, err := w.Replay()
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	for _, entry := range entries {
		if entry.Type == wal.OpPut {
			mt.Put(entry.Key, entry.Value)
		} else if entry.Type == wal.OpDelete {
			mt.Delete(entry.Key)
		}
	}

	// Load existing SSTables
	sstables, err := loadSSTables(path)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}

	db := &DB{
		path:      path,
		wal:       w,
		memtable:  mt,
		sstables:  sstables,
		closed:    false,
		flushChan: make(chan struct{}, 1),
	}

	return db, nil
}

// Put inserts or updates a key-value pair
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	db.mutex.Lock()
	if db.closed {
		db.mutex.Unlock()
		return fmt.Errorf("database is closed")
	}
	db.mutex.Unlock()

	// Write to WAL first (durability)
	if err := db.wal.Append(key, value, wal.OpPut); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Write to MemTable (performance)
	if err := db.memtable.Put(key, value); err != nil {
		return fmt.Errorf("failed to write to MemTable: %w", err)
	}

	// Check if MemTable needs flushing
	if db.memtable.IsFull() {
		// Trigger flush asynchronously
		select {
		case db.flushChan <- struct{}{}:
			go db.flushMemTable()
		default:
			// Flush already in progress
		}
	}

	return nil
}

// Get retrieves a value by key
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	db.mutex.RLock()
	if db.closed {
		db.mutex.RUnlock()
		return nil, fmt.Errorf("database is closed")
	}
	db.mutex.RUnlock()

	// Check MemTable first (newest data)
	value, found, err := db.memtable.Get(key)
	if err != nil {
		return nil, err
	}
	if found {
		// Check if it's a tombstone (deleted)
		if value == nil {
			return nil, fmt.Errorf("key not found")
		}
		return value, nil
	}

	// Check SSTables (newest to oldest)
	db.mutex.RLock()
	sstables := db.sstables
	db.mutex.RUnlock()

	for i := len(sstables) - 1; i >= 0; i-- {
		value, found, err := sstables[i].Get(key)
		if err != nil {
			return nil, fmt.Errorf("failed to read from SSTable: %w", err)
		}
		if found {
			// Check if it's a tombstone
			if len(value) == 4 && value[0] == 0xFF && value[1] == 0xFF &&
				value[2] == 0xFF && value[3] == 0xFF {
				return nil, fmt.Errorf("key not found")
			}
			return value, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// Delete marks a key as deleted
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	db.mutex.Lock()
	if db.closed {
		db.mutex.Unlock()
		return fmt.Errorf("database is closed")
	}
	db.mutex.Unlock()

	// Write tombstone to WAL
	if err := db.wal.Append(key, nil, wal.OpDelete); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Write tombstone to MemTable
	if err := db.memtable.Delete(key); err != nil {
		return fmt.Errorf("failed to delete from MemTable: %w", err)
	}

	// Check if MemTable needs flushing
	if db.memtable.IsFull() {
		select {
		case db.flushChan <- struct{}{}:
			go db.flushMemTable()
		default:
		}
	}

	return nil
}

// Close closes the database and releases resources
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.closed {
		return fmt.Errorf("database already closed")
	}

	db.closed = true

	// Close WAL
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// Close all SSTables
	for _, sst := range db.sstables {
		if err := sst.Close(); err != nil {
			return fmt.Errorf("failed to close SSTable: %w", err)
		}
	}

	return nil
}

// flushMemTable flushes the current MemTable to an SSTable
func (db *DB) flushMemTable() error {
	// Consume the flush signal at the end
	defer func() { <-db.flushChan }()
	
	db.mutex.Lock()
	
	// Get all entries from MemTable
	entries := db.memtable.GetAll()
	if len(entries) == 0 {
		db.mutex.Unlock()
		return nil
	}

	// Create new SSTable file
	timestamp := time.Now().UnixNano()
	sstPath := filepath.Join(db.path, fmt.Sprintf("table_%d.sst", timestamp))

	db.mutex.Unlock()

	// Write SSTable (outside of lock)
	writer, err := sstable.NewWriter(sstPath)
	if err != nil {
		return fmt.Errorf("failed to create SSTable writer: %w", err)
	}

	for _, entry := range entries {
		if err := writer.Add(entry.Key, entry.Value); err != nil {
			writer.Close()
			return fmt.Errorf("failed to write entry to SSTable: %w", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		writer.Close()
		return fmt.Errorf("failed to finalize SSTable: %w", err)
	}
	writer.Close()

	// Open the new SSTable for reading
	sst, err := sstable.Open(sstPath)
	if err != nil {
		return fmt.Errorf("failed to open SSTable: %w", err)
	}

	// Update database state (with lock)
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Add new SSTable to list FIRST
	db.sstables = append(db.sstables, sst)

	// THEN clear MemTable (data is now in SSTable)
	db.memtable.Clear()

	// Rotate WAL
	if err := db.wal.Rotate(); err != nil {
		return fmt.Errorf("failed to rotate WAL: %w", err)
	}

	return nil
}

// loadSSTables loads all existing SSTable files from the directory
func loadSSTables(path string) ([]*sstable.SSTable, error) {
	pattern := filepath.Join(path, "table_*.sst")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list SSTable files: %w", err)
	}

	sstables := make([]*sstable.SSTable, 0, len(files))
	for _, file := range files {
		sst, err := sstable.Open(file)
		if err != nil {
			// Close already opened SSTables
			for _, s := range sstables {
				s.Close()
			}
			return nil, fmt.Errorf("failed to open SSTable %s: %w", file, err)
		}
		sstables = append(sstables, sst)
	}

	return sstables, nil
}

// Stats returns database statistics
func (db *DB) Stats() map[string]interface{} {
	db.mutex.RLock()
	defer db.mutex.RUnlock()

	return map[string]interface{}{
		"memtable_size":    db.memtable.Size(),
		"memtable_entries": db.memtable.Len(),
		"sstable_count":    len(db.sstables),
		"path":             db.path,
	}
}

// Flush forces a flush of the current MemTable to SSTable
// This is primarily for testing purposes
func (db *DB) Flush() error {
	db.mutex.RLock()
	if db.closed {
		db.mutex.RUnlock()
		return fmt.Errorf("database is closed")
	}
	
	// Check if there's data to flush
	hasData := db.memtable.Len() > 0
	db.mutex.RUnlock()
	
	if !hasData {
		return nil
	}

	// Trigger flush synchronously
	// Try to send to channel, if it's full, a flush is already in progress
	select {
	case db.flushChan <- struct{}{}:
		// We got the token, do the flush
		return db.flushMemTable()
	default:
		// Channel is full, flush already in progress
		// Wait for it to complete by trying to send again (blocking)
		db.flushChan <- struct{}{}
		return db.flushMemTable()
	}
}
