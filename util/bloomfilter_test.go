package util

import (
	"fmt"
	"testing"
)

func TestBloomFilterBasic(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Add some keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	// Test that added keys are found
	for _, key := range keys {
		if !bf.MayContain(key) {
			t.Errorf("Expected key %s to be in bloom filter", string(key))
		}
	}

	// Test that non-existent keys are mostly not found
	// (some false positives are expected)
	nonExistentKeys := [][]byte{
		[]byte("nothere1"),
		[]byte("nothere2"),
		[]byte("nothere3"),
		[]byte("nothere4"),
		[]byte("nothere5"),
	}

	falsePositives := 0
	for _, key := range nonExistentKeys {
		if bf.MayContain(key) {
			falsePositives++
		}
	}

	// With 1% FPR and 5 checks, we expect 0-1 false positives typically
	if falsePositives > 2 {
		t.Errorf("Too many false positives: %d out of %d", falsePositives, len(nonExistentKeys))
	}
}

func TestBloomFilterSerialization(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Add some keys
	keys := [][]byte{
		[]byte("serialize1"),
		[]byte("serialize2"),
		[]byte("serialize3"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	// Serialize
	data := bf.Serialize()

	// Deserialize
	bf2, err := DeserializeBloomFilter(data)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Verify all keys are still found
	for _, key := range keys {
		if !bf2.MayContain(key) {
			t.Errorf("Expected key %s to be in deserialized bloom filter", string(key))
		}
	}

	// Verify metadata
	if bf2.NumEntries() != bf.NumEntries() {
		t.Errorf("NumEntries mismatch: got %d, want %d", bf2.NumEntries(), bf.NumEntries())
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	numEntries := uint64(1000)
	targetFPR := 0.01
	bf := NewBloomFilter(numEntries, targetFPR)

	// Add entries
	for i := uint64(0); i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		bf.Add(key)
	}

	// Test with non-existent keys
	testSize := 10000
	falsePositives := 0
	for i := 0; i < testSize; i++ {
		key := []byte(fmt.Sprintf("nonexistent%d", i))
		if bf.MayContain(key) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testSize)
	estimatedFPR := bf.EstimatedFalsePositiveRate()

	t.Logf("Target FPR: %.4f", targetFPR)
	t.Logf("Estimated FPR: %.4f", estimatedFPR)
	t.Logf("Actual FPR: %.4f (%d/%d)", actualFPR, falsePositives, testSize)

	// Actual FPR should be reasonably close to target (within 3x)
	if actualFPR > targetFPR*3 {
		t.Errorf("Actual FPR %.4f is too high (target: %.4f)", actualFPR, targetFPR)
	}
}

func TestBloomFilterEmpty(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Empty bloom filter should not contain any keys
	keys := [][]byte{
		[]byte("test1"),
		[]byte("test2"),
		[]byte("test3"),
	}

	for _, key := range keys {
		if bf.MayContain(key) {
			t.Errorf("Empty bloom filter should not contain key %s", string(key))
		}
	}
}

func TestBloomFilterLargeKeys(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Test with large keys
	largeKey := make([]byte, 1024)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	bf.Add(largeKey)

	if !bf.MayContain(largeKey) {
		t.Error("Large key should be found in bloom filter")
	}

	// Modify one byte
	largeKey[500] = ^largeKey[500]
	// This key should likely not be found (but false positive is possible)
	// We just verify it doesn't crash
	_ = bf.MayContain(largeKey)
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(uint64(b.N), 0.01)
	key := []byte("benchmarkkey")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(key)
	}
}

func BenchmarkBloomFilterMayContain(b *testing.B) {
	bf := NewBloomFilter(1000, 0.01)
	key := []byte("benchmarkkey")
	bf.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain(key)
	}
}
