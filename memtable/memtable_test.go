package memtable

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestMemTableBasicOperations(t *testing.T) {
	mt := New(1024 * 1024) // 1MB

	// Test Put
	err := mt.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	value, found, err := mt.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Key not found")
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("Expected value1, got %s", string(value))
	}

	// Test non-existent key
	_, found, err = mt.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Fatal("Non-existent key should not be found")
	}
}

func TestMemTableUpdate(t *testing.T) {
	mt := New(1024 * 1024)

	// Insert initial value
	mt.Put([]byte("key1"), []byte("value1"))

	// Update value
	mt.Put([]byte("key1"), []byte("value2"))

	// Verify updated value
	value, found, _ := mt.Get([]byte("key1"))
	if !found {
		t.Fatal("Key not found after update")
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Fatalf("Expected value2, got %s", string(value))
	}

	// Verify only one entry exists
	if mt.Len() != 1 {
		t.Fatalf("Expected 1 entry, got %d", mt.Len())
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := New(1024 * 1024)

	// Insert and delete
	mt.Put([]byte("key1"), []byte("value1"))
	err := mt.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify tombstone
	value, found, err := mt.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Deleted key should be found (as tombstone)")
	}
	if value != nil {
		t.Fatal("Deleted key should return nil value")
	}

	// Delete non-existent key
	err = mt.Delete([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Delete of non-existent key failed: %v", err)
	}
}

func TestMemTableSizeTracking(t *testing.T) {
	mt := New(1024)

	initialSize := mt.Size()
	if initialSize != 0 {
		t.Fatalf("Initial size should be 0, got %d", initialSize)
	}

	// Add entry
	mt.Put([]byte("key1"), []byte("value1"))
	size1 := mt.Size()
	if size1 <= 0 {
		t.Fatal("Size should increase after Put")
	}

	// Add another entry
	mt.Put([]byte("key2"), []byte("value2"))
	size2 := mt.Size()
	if size2 <= size1 {
		t.Fatal("Size should increase with more entries")
	}

	// Update existing entry with larger value
	mt.Put([]byte("key1"), []byte("much_longer_value"))
	size3 := mt.Size()
	if size3 <= size2 {
		t.Fatal("Size should increase when value grows")
	}

	// Update with smaller value
	mt.Put([]byte("key1"), []byte("short"))
	size4 := mt.Size()
	if size4 >= size3 {
		t.Fatal("Size should decrease when value shrinks")
	}
}

func TestMemTableIsFull(t *testing.T) {
	mt := New(100) // Small size for testing

	if mt.IsFull() {
		t.Fatal("Empty MemTable should not be full")
	}

	// Fill it up
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		mt.Put([]byte(key), []byte(value))
	}

	if !mt.IsFull() {
		t.Fatalf("MemTable should be full. Size: %d, MaxSize: %d", mt.Size(), mt.MaxSize())
	}
}

func TestMemTableGetAll(t *testing.T) {
	mt := New(1024 * 1024)

	// Insert entries in random order
	entries := map[string]string{
		"key3": "value3",
		"key1": "value1",
		"key2": "value2",
	}

	for k, v := range entries {
		mt.Put([]byte(k), []byte(v))
	}

	// Get all entries
	all := mt.GetAll()

	if len(all) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(all))
	}

	// Verify sorted order
	expectedOrder := []string{"key1", "key2", "key3"}
	for i, entry := range all {
		if string(entry.Key) != expectedOrder[i] {
			t.Fatalf("Expected key %s at position %d, got %s", expectedOrder[i], i, string(entry.Key))
		}
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := New(1024 * 1024)

	// Insert entries
	mt.Put([]byte("key1"), []byte("value1"))
	mt.Put([]byte("key2"), []byte("value2"))
	mt.Put([]byte("key3"), []byte("value3"))

	// Test iterator
	iter := mt.NewIterator()
	count := 0
	expectedKeys := []string{"key1", "key2", "key3"}

	for iter.Next() {
		if !iter.Valid() {
			t.Fatal("Iterator should be valid after Next()")
		}

		key := string(iter.Key())
		if key != expectedKeys[count] {
			t.Fatalf("Expected key %s, got %s", expectedKeys[count], key)
		}

		count++
	}

	if count != 3 {
		t.Fatalf("Expected to iterate over 3 entries, got %d", count)
	}
}

func TestMemTableIteratorSeek(t *testing.T) {
	mt := New(1024 * 1024)

	// Insert entries
	mt.Put([]byte("key1"), []byte("value1"))
	mt.Put([]byte("key3"), []byte("value3"))
	mt.Put([]byte("key5"), []byte("value5"))

	// Seek to key3
	iter := mt.NewIterator()
	iter.Seek([]byte("key3"))
	iter.Next()

	if !iter.Valid() {
		t.Fatal("Iterator should be valid after Seek")
	}

	if string(iter.Key()) != "key3" {
		t.Fatalf("Expected key3, got %s", string(iter.Key()))
	}

	// Continue iteration
	iter.Next()
	if string(iter.Key()) != "key5" {
		t.Fatalf("Expected key5, got %s", string(iter.Key()))
	}
}

func TestMemTableConcurrentReads(t *testing.T) {
	mt := New(1024 * 1024)

	// Populate with data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		mt.Put([]byte(key), []byte(value))
	}

	// Concurrent reads
	var wg sync.WaitGroup
	numReaders := 10
	readsPerReader := 100

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			for j := 0; j < readsPerReader; j++ {
				keyNum := j % 100
				key := fmt.Sprintf("key%03d", keyNum)
				expectedValue := fmt.Sprintf("value%03d", keyNum)

				value, found, err := mt.Get([]byte(key))
				if err != nil {
					t.Errorf("Reader %d: Get failed: %v", readerID, err)
					return
				}
				if !found {
					t.Errorf("Reader %d: Key %s not found", readerID, key)
					return
				}
				if string(value) != expectedValue {
					t.Errorf("Reader %d: Expected %s, got %s", readerID, expectedValue, string(value))
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestMemTableConcurrentWrites(t *testing.T) {
	mt := New(10 * 1024 * 1024) // 10MB

	var wg sync.WaitGroup
	numWriters := 10
	writesPerWriter := 100

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < writesPerWriter; j++ {
				key := fmt.Sprintf("writer%d_key%d", writerID, j)
				value := fmt.Sprintf("writer%d_value%d", writerID, j)

				err := mt.Put([]byte(key), []byte(value))
				if err != nil {
					t.Errorf("Writer %d: Put failed: %v", writerID, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all entries
	expectedEntries := numWriters * writesPerWriter
	if mt.Len() != expectedEntries {
		t.Fatalf("Expected %d entries, got %d", expectedEntries, mt.Len())
	}
}

func TestMemTableMixedConcurrentOperations(t *testing.T) {
	mt := New(10 * 1024 * 1024)

	// Pre-populate
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := fmt.Sprintf("value%03d", i)
		mt.Put([]byte(key), []byte(value))
	}

	var wg sync.WaitGroup

	// Readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key%03d", j%100)
				mt.Get([]byte(key))
			}
		}()
	}

	// Writers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key := fmt.Sprintf("newkey%d_%d", id, j)
				value := fmt.Sprintf("newvalue%d_%d", id, j)
				mt.Put([]byte(key), []byte(value))
			}
		}(i)
	}

	// Deleters
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				key := fmt.Sprintf("key%03d", j)
				mt.Delete([]byte(key))
			}
		}()
	}

	wg.Wait()
}

func TestMemTableEmptyKey(t *testing.T) {
	mt := New(1024 * 1024)

	// Test Put with empty key
	err := mt.Put([]byte{}, []byte("value"))
	if err == nil {
		t.Fatal("Put with empty key should fail")
	}

	// Test Get with empty key
	_, _, err = mt.Get([]byte{})
	if err == nil {
		t.Fatal("Get with empty key should fail")
	}

	// Test Delete with empty key
	err = mt.Delete([]byte{})
	if err == nil {
		t.Fatal("Delete with empty key should fail")
	}
}

func TestMemTableClear(t *testing.T) {
	mt := New(1024 * 1024)

	// Add entries
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		mt.Put([]byte(key), []byte(value))
	}

	if mt.Len() != 10 {
		t.Fatalf("Expected 10 entries, got %d", mt.Len())
	}

	// Clear
	mt.Clear()

	if mt.Len() != 0 {
		t.Fatalf("Expected 0 entries after clear, got %d", mt.Len())
	}

	if mt.Size() != 0 {
		t.Fatalf("Expected 0 size after clear, got %d", mt.Size())
	}

	// Verify entries are gone
	_, found, _ := mt.Get([]byte("key0"))
	if found {
		t.Fatal("Entry should not exist after clear")
	}
}

func TestMemTableLargeValues(t *testing.T) {
	mt := New(10 * 1024 * 1024) // 10MB

	// Create 1MB value
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Put large value
	err := mt.Put([]byte("large_key"), largeValue)
	if err != nil {
		t.Fatalf("Put large value failed: %v", err)
	}

	// Get large value
	value, found, err := mt.Get([]byte("large_key"))
	if err != nil {
		t.Fatalf("Get large value failed: %v", err)
	}
	if !found {
		t.Fatal("Large value not found")
	}
	if !bytes.Equal(value, largeValue) {
		t.Fatal("Large value corrupted")
	}
}

func BenchmarkMemTablePut(b *testing.B) {
	mt := New(100 * 1024 * 1024) // 100MB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		mt.Put([]byte(key), []byte(value))
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	mt := New(100 * 1024 * 1024)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		mt.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
		mt.Get([]byte(key))
	}
}

func BenchmarkMemTableConcurrentReads(b *testing.B) {
	mt := New(100 * 1024 * 1024)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		mt.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%10000)
			mt.Get([]byte(key))
			i++
		}
	})
}
