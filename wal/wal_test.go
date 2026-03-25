package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create WAL
	w, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Test data
	testCases := []struct {
		key   []byte
		value []byte
		op    OpType
	}{
		{[]byte("key1"), []byte("value1"), OpPut},
		{[]byte("key2"), []byte("value2"), OpPut},
		{[]byte("key3"), []byte("value3"), OpPut},
		{[]byte("key2"), nil, OpDelete},
	}

	// Append entries
	for _, tc := range testCases {
		if err := w.Append(tc.key, tc.value, tc.op); err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	// Replay entries
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify count
	if len(entries) != len(testCases) {
		t.Fatalf("Expected %d entries, got %d", len(testCases), len(entries))
	}

	// Verify each entry
	for i, tc := range testCases {
		entry := entries[i]

		if !bytes.Equal(entry.Key, tc.key) {
			t.Errorf("Entry %d: expected key %s, got %s", i, tc.key, entry.Key)
		}

		if !bytes.Equal(entry.Value, tc.value) {
			t.Errorf("Entry %d: expected value %s, got %s", i, tc.value, entry.Value)
		}

		if entry.Type != tc.op {
			t.Errorf("Entry %d: expected type %d, got %d", i, tc.op, entry.Type)
		}

		if entry.Timestamp == 0 {
			t.Errorf("Entry %d: timestamp should not be zero", i)
		}
	}
}

func TestWAL_Recovery(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create WAL and write some entries
	w1, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	testData := []struct {
		key   string
		value string
	}{
		{"user:1", "Alice"},
		{"user:2", "Bob"},
		{"user:3", "Charlie"},
	}

	for _, td := range testData {
		if err := w1.Append([]byte(td.key), []byte(td.value), OpPut); err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	// Close WAL
	if err := w1.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Reopen WAL (simulating recovery)
	w2, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	// Replay entries
	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	// Verify all entries recovered
	if len(entries) != len(testData) {
		t.Fatalf("Expected %d entries, got %d", len(testData), len(entries))
	}

	for i, td := range testData {
		if string(entries[i].Key) != td.key {
			t.Errorf("Entry %d: expected key %s, got %s", i, td.key, entries[i].Key)
		}
		if string(entries[i].Value) != td.value {
			t.Errorf("Entry %d: expected value %s, got %s", i, td.value, entries[i].Value)
		}
	}
}

func TestWAL_Rotate(t *testing.T) {
	// Create temporary directory for test
	tmpDir := t.TempDir()

	// Create WAL
	w, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write some entries
	if err := w.Append([]byte("key1"), []byte("value1"), OpPut); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	// Rotate WAL
	if err := w.Rotate(); err != nil {
		t.Fatalf("Failed to rotate WAL: %v", err)
	}

	// Check that archived file exists
	files, err := filepath.Glob(filepath.Join(tmpDir, "wal.current.*.old"))
	if err != nil {
		t.Fatalf("Failed to list files: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("Expected 1 archived file, got %d", len(files))
	}

	// Write to new WAL
	if err := w.Append([]byte("key2"), []byte("value2"), OpPut); err != nil {
		t.Fatalf("Failed to append after rotation: %v", err)
	}

	// Replay should only show new entry
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry after rotation, got %d", len(entries))
	}

	if string(entries[0].Key) != "key2" {
		t.Errorf("Expected key2, got %s", entries[0].Key)
	}
}

func TestWAL_DeleteOperation(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Put then delete
	if err := w.Append([]byte("key1"), []byte("value1"), OpPut); err != nil {
		t.Fatalf("Failed to append put: %v", err)
	}

	if err := w.Append([]byte("key1"), nil, OpDelete); err != nil {
		t.Fatalf("Failed to append delete: %v", err)
	}

	// Replay
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Verify delete entry
	deleteEntry := entries[1]
	if deleteEntry.Type != OpDelete {
		t.Errorf("Expected OpDelete, got %d", deleteEntry.Type)
	}

	if len(deleteEntry.Value) != 0 {
		t.Errorf("Delete entry should have empty value")
	}
}

func TestWAL_LargeValues(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Create large value (1MB)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Append large entry
	if err := w.Append([]byte("large_key"), largeValue, OpPut); err != nil {
		t.Fatalf("Failed to append large entry: %v", err)
	}

	// Replay and verify
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if !bytes.Equal(entries[0].Value, largeValue) {
		t.Errorf("Large value corrupted during write/read")
	}
}

func TestWAL_CorruptedEntry(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write valid entries
	if err := w.Append([]byte("key1"), []byte("value1"), OpPut); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	if err := w.Append([]byte("key2"), []byte("value2"), OpPut); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	w.Close()

	// Corrupt the WAL file by truncating it
	walPath := filepath.Join(tmpDir, "wal.current")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL: %v", err)
	}

	// Truncate to remove last few bytes
	if err := os.Truncate(walPath, info.Size()-10); err != nil {
		t.Fatalf("Failed to truncate WAL: %v", err)
	}

	// Reopen and replay
	w2, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	// Should recover first entry only
	if len(entries) != 1 {
		t.Fatalf("Expected 1 valid entry, got %d", len(entries))
	}

	if string(entries[0].Key) != "key1" {
		t.Errorf("Expected key1, got %s", entries[0].Key)
	}
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Concurrent writes
	done := make(chan bool)
	numGoroutines := 10
	writesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < writesPerGoroutine; j++ {
				key := []byte("key")
				value := []byte("value")
				if err := w.Append(key, value, OpPut); err != nil {
					t.Errorf("Failed to append: %v", err)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all writes
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Failed to replay: %v", err)
	}

	expectedCount := numGoroutines * writesPerGoroutine
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}
}
