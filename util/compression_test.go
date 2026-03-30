package util

import (
	"bytes"
	"testing"
)

func TestCompressionNoCompression(t *testing.T) {
	data := []byte("Hello, World!")
	
	compressed, err := Compress(data, NoCompression)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}
	
	if !bytes.Equal(compressed, data) {
		t.Errorf("NoCompression should return original data")
	}
	
	decompressed, err := Decompress(compressed, NoCompression)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}
	
	if !bytes.Equal(decompressed, data) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

func TestCompressionFlate(t *testing.T) {
	data := []byte("Hello, World! This is a test string that should compress well because it has repetition. Hello, World!")
	
	compressed, err := Compress(data, FlateCompression)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}
	
	// Compressed data should be smaller for repetitive text
	if len(compressed) >= len(data) {
		t.Logf("Warning: Compressed size (%d) >= original size (%d)", len(compressed), len(data))
	}
	
	decompressed, err := Decompress(compressed, FlateCompression)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}
	
	if !bytes.Equal(decompressed, data) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

func TestCompressionLargeData(t *testing.T) {
	// Create 1MB of repetitive data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	
	compressed, err := Compress(data, FlateCompression)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}
	
	t.Logf("Original size: %d bytes, Compressed size: %d bytes, Ratio: %.2f%%",
		len(data), len(compressed), float64(len(compressed))/float64(len(data))*100)
	
	decompressed, err := Decompress(compressed, FlateCompression)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}
	
	if !bytes.Equal(decompressed, data) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

func TestCompressionEmptyData(t *testing.T) {
	data := []byte{}
	
	compressed, err := Compress(data, FlateCompression)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}
	
	decompressed, err := Decompress(compressed, FlateCompression)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}
	
	if !bytes.Equal(decompressed, data) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

func TestCompressionInvalidType(t *testing.T) {
	data := []byte("test")
	
	_, err := Compress(data, CompressionType(99))
	if err == nil {
		t.Error("Expected error for invalid compression type")
	}
	
	_, err = Decompress(data, CompressionType(99))
	if err == nil {
		t.Error("Expected error for invalid compression type")
	}
}

func TestCompressionRatio(t *testing.T) {
	tests := []struct {
		original   int
		compressed int
		expected   float64
	}{
		{100, 50, 0.5},
		{1000, 250, 0.25},
		{0, 0, 0},
		{100, 100, 1.0},
	}
	
	for _, tt := range tests {
		ratio := CompressionRatio(tt.original, tt.compressed)
		if ratio != tt.expected {
			t.Errorf("CompressionRatio(%d, %d) = %f, want %f",
				tt.original, tt.compressed, ratio, tt.expected)
		}
	}
}

func BenchmarkCompressionFlate(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Compress(data, FlateCompression)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecompressionFlate(b *testing.B) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	
	compressed, err := Compress(data, FlateCompression)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Decompress(compressed, FlateCompression)
		if err != nil {
			b.Fatal(err)
		}
	}
}
