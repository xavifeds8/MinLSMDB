package db

import (
	"bytes"
	"fmt"
	"testing"
)

func TestDBOpenAndClose(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestDBPutAndGet(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Put a key-value pair
	key := []byte("test_key")
	value := []byte("test_value")

	if err := db.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the value
	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(result, value) {
		t.Fatalf("Expected %s, got %s", value, result)
	}
}

func TestDBDelete(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Put and then delete
	key := []byte("test_key")
	value := []byte("test_value")

	db.Put(key, value)
	
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Try to get deleted key
	_, err = db.Get(key)
	if err == nil {
		t.Fatal("Expected error for deleted key")
	}
}

func TestDBUpdate(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	key := []byte("test_key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Put initial value
	db.Put(key, value1)

	// Update value
	db.Put(key, value2)

	// Get updated value
	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(result, value2) {
		t.Fatalf("Expected %s, got %s", value2, result)
	}
}

func TestDBNonExistentKey(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Get([]byte("nonexistent"))
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}
}

func TestDBEmptyKey(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Try to put with empty key
	err = db.Put([]byte{}, []byte("value"))
	if err == nil {
		t.Fatal("Expected error for empty key")
	}

	// Try to get with empty key
	_, err = db.Get([]byte{})
	if err == nil {
		t.Fatal("Expected error for empty key")
	}

	// Try to delete with empty key
	err = db.Delete([]byte{})
	if err == nil {
		t.Fatal("Expected error for empty key")
	}
}

func TestDBRecovery(t *testing.T) {
	dir := t.TempDir()

	// Open database and write data
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")

	db.Put(key1, value1)
	db.Put(key2, value2)
	db.Close()

	// Reopen database (should recover from WAL)
	db, err = Open(dir)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify data
	result1, err := db.Get(key1)
	if err != nil {
		t.Fatalf("Get failed after recovery: %v", err)
	}
	if !bytes.Equal(result1, value1) {
		t.Fatalf("Expected %s, got %s", value1, result1)
	}

	result2, err := db.Get(key2)
	if err != nil {
		t.Fatalf("Get failed after recovery: %v", err)
	}
	if !bytes.Equal(result2, value2) {
		t.Fatalf("Expected %s, got %s", value2, result2)
	}
}

func TestDBFlush(t *testing.T) {
	dir := t.TempDir()

	// Open with small MemTable size to trigger flush
	opts := Options{
		MemTableSize: 1024, // 1KB
	}
	db, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write enough data to trigger flush
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Force flush and wait for completion
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify data is still accessible
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		expectedValue := []byte(fmt.Sprintf("value%03d", i))

		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get failed after flush: %v", err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("Expected %s, got %s", expectedValue, value)
		}
	}

	// Check that SSTable was created
	stats := db.Stats()
	sstableCount := stats["sstable_count"].(int)
	if sstableCount == 0 {
		t.Fatal("Expected at least one SSTable after flush")
	}
}

func TestDBMultipleSSTables(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		MemTableSize: 512, // Small size to create multiple SSTables
	}
	db, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write data in batches to create multiple SSTables
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("batch%d_key%03d", batch, i))
			value := []byte(fmt.Sprintf("batch%d_value%03d", batch, i))
			db.Put(key, value)
		}
		// Force flush and wait for completion
		if err := db.Flush(); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	// Verify all data is accessible
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("batch%d_key%03d", batch, i))
			expectedValue := []byte(fmt.Sprintf("batch%d_value%03d", batch, i))

			value, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if !bytes.Equal(value, expectedValue) {
				t.Fatalf("Expected %s, got %s", expectedValue, value)
			}
		}
	}
}

func TestDBStats(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write some data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		db.Put(key, value)
	}

	stats := db.Stats()

	if stats["memtable_entries"].(int) != 10 {
		t.Fatalf("Expected 10 entries, got %d", stats["memtable_entries"])
	}

	if stats["path"].(string) != dir {
		t.Fatalf("Expected path %s, got %s", dir, stats["path"])
	}
}

func TestDBClosedOperations(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	// Try operations on closed database
	err = db.Put([]byte("key"), []byte("value"))
	if err == nil {
		t.Fatal("Expected error for Put on closed database")
	}

	_, err = db.Get([]byte("key"))
	if err == nil {
		t.Fatal("Expected error for Get on closed database")
	}

	err = db.Delete([]byte("key"))
	if err == nil {
		t.Fatal("Expected error for Delete on closed database")
	}
}

func TestDBLargeValues(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	key := []byte("large_key")
	if err := db.Put(key, largeValue); err != nil {
		t.Fatalf("Put large value failed: %v", err)
	}

	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get large value failed: %v", err)
	}

	if !bytes.Equal(result, largeValue) {
		t.Fatal("Large value corrupted")
	}
}

func TestDBConcurrentReads(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write test data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		db.Put(key, value)
	}

	// Concurrent reads
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key%03d", j))
				expectedValue := []byte(fmt.Sprintf("value%03d", j))

				value, err := db.Get(key)
				if err != nil {
					t.Errorf("Reader %d: Get failed: %v", id, err)
					done <- false
					return
				}
				if !bytes.Equal(value, expectedValue) {
					t.Errorf("Reader %d: Expected %s, got %s", id, expectedValue, value)
					done <- false
					return
				}
			}
			done <- true
		}(i)
	}

	// Wait for all readers
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestDBPersistence(t *testing.T) {
	dir := t.TempDir()

	// Create database and write data
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	testData := map[string]string{
		"user:1": "Alice",
		"user:2": "Bob",
		"user:3": "Charlie",
	}

	for k, v := range testData {
		db.Put([]byte(k), []byte(v))
	}

	db.Close()

	// Reopen and verify
	db, err = Open(dir)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	for k, expectedV := range testData {
		v, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get failed for %s: %v", k, err)
		}
		if string(v) != expectedV {
			t.Fatalf("Expected %s, got %s", expectedV, string(v))
		}
	}
}

func BenchmarkDBPut(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		db.Put(key, value)
	}
}

func BenchmarkDBGet(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(dir)
	defer db.Close()

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		db.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i%10000))
		db.Get(key)
	}
}
