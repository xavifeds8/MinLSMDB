package sstable

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	// Block size for data blocks (4KB)
	BlockSize = 4096

	// Magic number to identify SSTable files
	MagicNumber = 0x5354424C // "STBL" in hex

	// Version of the SSTable format
	Version = 1
)

// Entry represents a key-value pair in the SSTable
type Entry struct {
	Key   []byte
	Value []byte
}

// IndexEntry represents an entry in the sparse index
type IndexEntry struct {
	Key    []byte
	Offset int64
}

// Footer contains metadata about the SSTable
type Footer struct {
	IndexOffset int64
	IndexSize   int64
	NumEntries  int64
	MagicNumber uint32
	Version     uint32
}

// SSTable represents a sorted string table on disk
type SSTable struct {
	path       string
	file       *os.File
	indexCache []IndexEntry
	footer     Footer
}

// Writer is used to create a new SSTable
type Writer struct {
	file         *os.File
	path         string
	currentBlock []byte
	blockOffset  int64
	index        []IndexEntry
	numEntries   int64
	firstKeyInBlock []byte
}

// NewWriter creates a new SSTable writer
func NewWriter(path string) (*Writer, error) {
	// Create parent directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	return &Writer{
		file:        file,
		path:        path,
		currentBlock: make([]byte, 0, BlockSize),
		blockOffset: 0,
		index:       make([]IndexEntry, 0),
		numEntries:  0,
	}, nil
}

// Add adds a key-value pair to the SSTable
// Keys must be added in sorted order
func (w *Writer) Add(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	// Encode entry: [keySize(4)][valueSize(4)][key][value]
	entrySize := 4 + 4 + len(key) + len(value)
	entry := make([]byte, entrySize)

	offset := 0
	binary.BigEndian.PutUint32(entry[offset:], uint32(len(key)))
	offset += 4
	binary.BigEndian.PutUint32(entry[offset:], uint32(len(value)))
	offset += 4
	copy(entry[offset:], key)
	offset += len(key)
	copy(entry[offset:], value)

	// If adding this entry would exceed block size, flush current block
	if len(w.currentBlock)+entrySize > BlockSize && len(w.currentBlock) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	// If this is the first entry in the block, record it for the index
	if len(w.currentBlock) == 0 {
		w.firstKeyInBlock = make([]byte, len(key))
		copy(w.firstKeyInBlock, key)
	}

	// Add entry to current block
	w.currentBlock = append(w.currentBlock, entry...)
	w.numEntries++

	return nil
}

// flushBlock writes the current block to disk and updates the index
func (w *Writer) flushBlock() error {
	if len(w.currentBlock) == 0 {
		return nil
	}

	// Pad block to BlockSize
	if len(w.currentBlock) < BlockSize {
		padding := make([]byte, BlockSize-len(w.currentBlock))
		w.currentBlock = append(w.currentBlock, padding...)
	}

	// Calculate CRC32 for the block
	crc := crc32.ChecksumIEEE(w.currentBlock)

	// Write CRC32 (4 bytes)
	crcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBytes, crc)
	if _, err := w.file.Write(crcBytes); err != nil {
		return fmt.Errorf("failed to write CRC: %w", err)
	}

	// Write block
	if _, err := w.file.Write(w.currentBlock); err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	// Add to index (first key in block → block offset)
	w.index = append(w.index, IndexEntry{
		Key:    w.firstKeyInBlock,
		Offset: w.blockOffset,
	})

	// Update offset (CRC + block)
	w.blockOffset += 4 + int64(len(w.currentBlock))

	// Reset current block
	w.currentBlock = w.currentBlock[:0]
	w.firstKeyInBlock = nil

	return nil
}

// Finalize completes the SSTable by writing the index and footer
func (w *Writer) Finalize() error {
	// Flush any remaining data in current block
	if err := w.flushBlock(); err != nil {
		return err
	}

	// Record index offset
	indexOffset := w.blockOffset

	// Write index
	for _, entry := range w.index {
		// Format: [keySize(4)][key][offset(8)]
		indexEntry := make([]byte, 4+len(entry.Key)+8)
		offset := 0

		binary.BigEndian.PutUint32(indexEntry[offset:], uint32(len(entry.Key)))
		offset += 4
		copy(indexEntry[offset:], entry.Key)
		offset += len(entry.Key)
		binary.BigEndian.PutUint64(indexEntry[offset:], uint64(entry.Offset))

		if _, err := w.file.Write(indexEntry); err != nil {
			return fmt.Errorf("failed to write index entry: %w", err)
		}
	}

	// Calculate index size
	currentPos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current position: %w", err)
	}
	indexSize := currentPos - indexOffset

	// Write footer
	footer := Footer{
		IndexOffset: indexOffset,
		IndexSize:   indexSize,
		NumEntries:  w.numEntries,
		MagicNumber: MagicNumber,
		Version:     Version,
	}

	footerBytes := make([]byte, 32) // 8+8+8+4+4 = 32 bytes
	offset := 0
	binary.BigEndian.PutUint64(footerBytes[offset:], uint64(footer.IndexOffset))
	offset += 8
	binary.BigEndian.PutUint64(footerBytes[offset:], uint64(footer.IndexSize))
	offset += 8
	binary.BigEndian.PutUint64(footerBytes[offset:], uint64(footer.NumEntries))
	offset += 8
	binary.BigEndian.PutUint32(footerBytes[offset:], footer.MagicNumber)
	offset += 4
	binary.BigEndian.PutUint32(footerBytes[offset:], footer.Version)

	if _, err := w.file.Write(footerBytes); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

// Close closes the writer
func (w *Writer) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Path returns the path of the SSTable file
func (w *Writer) Path() string {
	return w.path
}

// Open opens an existing SSTable for reading
func Open(path string) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Read footer (last 32 bytes)
	if _, err := file.Seek(-32, io.SeekEnd); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to footer: %w", err)
	}

	footerBytes := make([]byte, 32)
	if _, err := io.ReadFull(file, footerBytes); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	// Parse footer
	footer := Footer{
		IndexOffset: int64(binary.BigEndian.Uint64(footerBytes[0:8])),
		IndexSize:   int64(binary.BigEndian.Uint64(footerBytes[8:16])),
		NumEntries:  int64(binary.BigEndian.Uint64(footerBytes[16:24])),
		MagicNumber: binary.BigEndian.Uint32(footerBytes[24:28]),
		Version:     binary.BigEndian.Uint32(footerBytes[28:32]),
	}

	// Verify magic number
	if footer.MagicNumber != MagicNumber {
		file.Close()
		return nil, fmt.Errorf("invalid magic number: expected %x, got %x", MagicNumber, footer.MagicNumber)
	}

	// Verify version
	if footer.Version != Version {
		file.Close()
		return nil, fmt.Errorf("unsupported version: %d", footer.Version)
	}

	// Read index
	if _, err := file.Seek(footer.IndexOffset, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to index: %w", err)
	}

	indexData := make([]byte, footer.IndexSize)
	if _, err := io.ReadFull(file, indexData); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read index: %w", err)
	}

	// Parse index
	index := make([]IndexEntry, 0)
	offset := 0
	for offset < len(indexData) {
		if offset+4 > len(indexData) {
			break
		}

		keySize := int(binary.BigEndian.Uint32(indexData[offset:]))
		offset += 4

		if offset+keySize+8 > len(indexData) {
			break
		}

		key := make([]byte, keySize)
		copy(key, indexData[offset:offset+keySize])
		offset += keySize

		blockOffset := int64(binary.BigEndian.Uint64(indexData[offset:]))
		offset += 8

		index = append(index, IndexEntry{
			Key:    key,
			Offset: blockOffset,
		})
	}

	return &SSTable{
		path:       path,
		file:       file,
		indexCache: index,
		footer:     footer,
	}, nil
}

// Get retrieves a value by key from the SSTable
func (s *SSTable) Get(key []byte) ([]byte, bool, error) {
	// Binary search in index to find the block
	blockIdx := s.findBlock(key)
	if blockIdx < 0 {
		return nil, false, nil
	}

	// Read the block
	block, err := s.readBlock(s.indexCache[blockIdx].Offset)
	if err != nil {
		return nil, false, err
	}

	// Search within the block
	value, found := s.searchInBlock(block, key)
	return value, found, nil
}

// findBlock uses binary search to find which block might contain the key
func (s *SSTable) findBlock(key []byte) int {
	left, right := 0, len(s.indexCache)-1
	result := -1

	for left <= right {
		mid := (left + right) / 2
		cmp := compareBytes(key, s.indexCache[mid].Key)

		if cmp >= 0 {
			result = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return result
}

// readBlock reads a block from disk
func (s *SSTable) readBlock(offset int64) ([]byte, error) {
	// Seek to block position
	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to block: %w", err)
	}

	// Read CRC32
	crcBytes := make([]byte, 4)
	if _, err := io.ReadFull(s.file, crcBytes); err != nil {
		return nil, fmt.Errorf("failed to read CRC: %w", err)
	}
	expectedCRC := binary.BigEndian.Uint32(crcBytes)

	// Read block
	block := make([]byte, BlockSize)
	if _, err := io.ReadFull(s.file, block); err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	// Verify CRC32
	actualCRC := crc32.ChecksumIEEE(block)
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, actualCRC)
	}

	return block, nil
}

// searchInBlock searches for a key within a block
func (s *SSTable) searchInBlock(block []byte, key []byte) ([]byte, bool) {
	offset := 0

	for offset < len(block) {
		// Check if we've reached padding
		if offset+8 > len(block) {
			break
		}

		// Read key size
		keySize := int(binary.BigEndian.Uint32(block[offset:]))
		offset += 4

		// Check for padding (keySize = 0)
		if keySize == 0 {
			break
		}

		// Read value size
		if offset+4 > len(block) {
			break
		}
		valueSize := int(binary.BigEndian.Uint32(block[offset:]))
		offset += 4

		// Check bounds
		if offset+keySize+valueSize > len(block) {
			break
		}

		// Read key
		entryKey := block[offset : offset+keySize]
		offset += keySize

		// Read value
		entryValue := block[offset : offset+valueSize]
		offset += valueSize

		// Compare keys
		if compareBytes(key, entryKey) == 0 {
			// Found it
			result := make([]byte, valueSize)
			copy(result, entryValue)
			return result, true
		}
	}

	return nil, false
}

// Close closes the SSTable
func (s *SSTable) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Path returns the path of the SSTable file
func (s *SSTable) Path() string {
	return s.path
}

// NumEntries returns the number of entries in the SSTable
func (s *SSTable) NumEntries() int64 {
	return s.footer.NumEntries
}

// compareBytes compares two byte slices lexicographically
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}

	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
