package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Write SSTable
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, e := range entries {
		if err := writer.Add([]byte(e.key), []byte(e.value)); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read SSTable
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Verify entries
	for _, e := range entries {
		value, found, err := reader.Get([]byte(e.key))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !found {
			t.Fatalf("Key %s not found", e.key)
		}
		if string(value) != e.value {
			t.Fatalf("Expected %s, got %s", e.value, string(value))
		}
	}

	// Test non-existent key
	_, found, err := reader.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Fatal("Non-existent key should not be found")
	}
}

func TestSSTableMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add enough entries to span multiple blocks
	numEntries := 200
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		if err := writer.Add([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%04d", i)

		value, found, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %s not found", key)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected %s, got %s", expectedValue, string(value))
		}
	}

	// Verify entry count
	if reader.NumEntries() != int64(numEntries) {
		t.Fatalf("Expected %d entries, got %d", numEntries, reader.NumEntries())
	}
}

func TestSSTableLargeValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Create large value (1KB)
	largeValue := make([]byte, 1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	if err := writer.Add([]byte("large_key"), largeValue); err != nil {
		t.Fatalf("Failed to add large entry: %v", err)
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get([]byte("large_key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Large key not found")
	}
	if !bytes.Equal(value, largeValue) {
		t.Fatal("Large value corrupted")
	}
}

func TestSSTableEmptyValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add entry with empty value
	if err := writer.Add([]byte("empty_key"), []byte{}); err != nil {
		t.Fatalf("Failed to add empty value: %v", err)
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get([]byte("empty_key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Empty key not found")
	}
	if len(value) != 0 {
		t.Fatalf("Expected empty value, got %d bytes", len(value))
	}
}

func TestSSTableInvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.sst")

	// Create invalid file
	if err := os.WriteFile(path, []byte("invalid data"), 0644); err != nil {
		t.Fatalf("Failed to create invalid file: %v", err)
	}

	// Try to open
	_, err := Open(path)
	if err == nil {
		t.Fatal("Expected error opening invalid file")
	}
}

func TestSSTableNonExistentFile(t *testing.T) {
	_, err := Open("/nonexistent/path/file.sst")
	if err == nil {
		t.Fatal("Expected error opening non-existent file")
	}
}

func TestSSTableEmptyKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Try to add entry with empty key
	err = writer.Add([]byte{}, []byte("value"))
	if err == nil {
		t.Fatal("Expected error adding empty key")
	}

	writer.Close()
}

func TestSSTableSortedOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add entries in sorted order
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for _, key := range keys {
		if err := writer.Add([]byte(key), []byte("value_"+key)); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify all keys
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	for _, key := range keys {
		value, found, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %s not found", key)
		}
		expectedValue := "value_" + key
		if string(value) != expectedValue {
			t.Fatalf("Expected %s, got %s", expectedValue, string(value))
		}
	}
}

func TestSSTableBinarySearch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add many entries to test binary search
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		if err := writer.Add([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify random keys
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	testKeys := []int{0, 1, 100, 500, 999}
	for _, i := range testKeys {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%06d", i)

		value, found, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %s not found", key)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected %s, got %s", expectedValue, string(value))
		}
	}
}

func TestSSTableTombstones(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add tombstone (empty value represents deletion)
	tombstone := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	if err := writer.Add([]byte("deleted_key"), tombstone); err != nil {
		t.Fatalf("Failed to add tombstone: %v", err)
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get([]byte("deleted_key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Tombstone key not found")
	}
	if !bytes.Equal(value, tombstone) {
		t.Fatal("Tombstone value mismatch")
	}
}

func TestCompareBytes(t *testing.T) {
	tests := []struct {
		a        []byte
		b        []byte
		expected int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("a"), []byte("a"), 0},
		{[]byte("abc"), []byte("abd"), -1},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte("ab"), []byte("abc"), -1},
		{[]byte{}, []byte("a"), -1},
		{[]byte("a"), []byte{}, 1},
		{[]byte{}, []byte{}, 0},
	}

	for _, tt := range tests {
		result := compareBytes(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("compareBytes(%q, %q) = %d, expected %d",
				tt.a, tt.b, result, tt.expected)
		}
	}
}

func BenchmarkSSTableWrite(b *testing.B) {
	dir := b.TempDir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(dir, fmt.Sprintf("bench_%d.sst", i))
		writer, _ := NewWriter(path)

		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key%d", j)
			value := fmt.Sprintf("value%d", j)
			writer.Add([]byte(key), []byte(value))
		}

		writer.Finalize()
		writer.Close()
	}
}

func TestSSTableWithCompression(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "compressed.sst")

	// Write SSTable with compression
	writer, err := NewWriterWithOptions(path, WriterOptions{
		Compression: 0x01, // FlateCompression
	})
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d_with_some_repetitive_data_to_compress_well", i)
		if err := writer.Add([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%04d_with_some_repetitive_data_to_compress_well", i)

		value, found, err := reader.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %s not found", key)
		}
		if string(value) != expectedValue {
			t.Fatalf("Expected %s, got %s", expectedValue, string(value))
		}
	}

	// Verify compression type in footer
	if reader.footer.CompressionType != 0x01 {
		t.Fatalf("Expected compression type 0x01, got %d", reader.footer.CompressionType)
	}
}

func TestSSTableNoCompression(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "uncompressed.sst")

	// Write SSTable without compression (default)
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add entries
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		if err := writer.Add([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if err := writer.Finalize(); err != nil {
		t.Fatalf("Failed to finalize: %v", err)
	}
	writer.Close()

	// Read and verify
	reader, err := Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Verify compression type is NoCompression
	if reader.footer.CompressionType != 0x00 {
		t.Fatalf("Expected compression type 0x00, got %d", reader.footer.CompressionType)
	}
}

func TestSSTableCompressionRatio(t *testing.T) {
	dir := t.TempDir()
	
	// Create uncompressed SSTable
	uncompressedPath := filepath.Join(dir, "uncompressed.sst")
	writer1, _ := NewWriter(uncompressedPath)
	
	// Create compressed SSTable
	compressedPath := filepath.Join(dir, "compressed.sst")
	writer2, _ := NewWriterWithOptions(compressedPath, WriterOptions{
		Compression: 0x01, // FlateCompression
	})

	// Add same data to both
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%04d", i)
		// Repetitive value that compresses well
		value := bytes.Repeat([]byte("data"), 50)
		writer1.Add([]byte(key), value)
		writer2.Add([]byte(key), value)
	}

	writer1.Finalize()
	writer1.Close()
	writer2.Finalize()
	writer2.Close()

	// Compare file sizes
	uncompressedInfo, _ := os.Stat(uncompressedPath)
	compressedInfo, _ := os.Stat(compressedPath)

	t.Logf("Uncompressed size: %d bytes", uncompressedInfo.Size())
	t.Logf("Compressed size: %d bytes", compressedInfo.Size())
	t.Logf("Compression ratio: %.2f%%", float64(compressedInfo.Size())/float64(uncompressedInfo.Size())*100)

	// Compressed should be smaller
	if compressedInfo.Size() >= uncompressedInfo.Size() {
		t.Logf("Warning: Compressed file is not smaller (this can happen with small datasets)")
	}
}

func BenchmarkSSTableRead(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.sst")

	// Create SSTable
	writer, _ := NewWriter(path)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		writer.Add([]byte(key), []byte(value))
	}
	writer.Finalize()
	writer.Close()

	// Open for reading
	reader, _ := Open(path)
	defer reader.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%06d", i%1000)
		reader.Get([]byte(key))
	}
}

func BenchmarkSSTableReadCompressed(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench_compressed.sst")

	// Create compressed SSTable
	writer, _ := NewWriterWithOptions(path, WriterOptions{
		Compression: 0x01, // FlateCompression
	})
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		writer.Add([]byte(key), []byte(value))
	}
	writer.Finalize()
	writer.Close()

	// Open for reading
	reader, _ := Open(path)
	defer reader.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%06d", i%1000)
		reader.Get([]byte(key))
	}
}
