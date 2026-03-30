package util

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
)

// CompressionType represents the compression algorithm used
type CompressionType byte

const (
	// NoCompression indicates no compression is applied
	NoCompression CompressionType = 0x00
	// FlateCompression indicates DEFLATE compression (zlib)
	FlateCompression CompressionType = 0x01
)

// Compress compresses data using the specified compression type
func Compress(data []byte, compressionType CompressionType) ([]byte, error) {
	switch compressionType {
	case NoCompression:
		return data, nil
	case FlateCompression:
		return compressFlate(data)
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// Decompress decompresses data using the specified compression type
func Decompress(data []byte, compressionType CompressionType) ([]byte, error) {
	switch compressionType {
	case NoCompression:
		return data, nil
	case FlateCompression:
		return decompressFlate(data)
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// compressFlate compresses data using DEFLATE algorithm
func compressFlate(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	// Use best speed for better write performance
	writer, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("failed to create flate writer: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to compress data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close flate writer: %w", err)
	}

	return buf.Bytes(), nil
}

// decompressFlate decompresses data using DEFLATE algorithm
func decompressFlate(data []byte) ([]byte, error) {
	reader := flate.NewReader(bytes.NewReader(data))
	defer reader.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	return buf.Bytes(), nil
}

// CompressionRatio calculates the compression ratio
func CompressionRatio(original, compressed int) float64 {
	if original == 0 {
		return 0
	}
	return float64(compressed) / float64(original)
}
