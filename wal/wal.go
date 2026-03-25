package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// OpType represents the type of operation in the WAL
type OpType byte

const (
	OpPut    OpType = 0x01
	OpDelete OpType = 0x02
)

// Entry represents a single WAL entry
type Entry struct {
	Timestamp int64
	Key       []byte
	Value     []byte
	Type      OpType
}

// WAL represents a Write-Ahead Log
type WAL struct {
	file  *os.File
	path  string
	mutex sync.Mutex
}

// New creates a new WAL instance
func New(dirPath string) (*WAL, error) {
	// Ensure directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Create WAL file path
	walPath := filepath.Join(dirPath, "wal.current")

	// Open or create WAL file with append mode
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file: file,
		path: walPath,
	}, nil
}

// Append adds a new entry to the WAL
// This implementation uses sync-per-write for maximum durability
func (w *WAL) Append(key, value []byte, opType OpType) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	entry := Entry{
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
		Type:      opType,
	}

	// Encode the entry
	data, err := encodeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}

	// Write to file
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Sync to disk (fsync) - ensures durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// Replay reads all entries from the WAL and returns them
// Used during recovery to rebuild the MemTable
func (w *WAL) Replay() ([]Entry, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Seek to beginning of file
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to beginning: %w", err)
	}

	var entries []Entry

	for {
		entry, err := readEntry(w.file)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Log error but stop replay at corruption point
			fmt.Printf("WAL replay stopped at corruption: %v\n", err)
			break
		}

		entries = append(entries, entry)
	}

	// Seek back to end for future appends
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return entries, nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	return nil
}

// Rotate creates a new WAL file and archives the old one
// Called after MemTable flush to SSTable
func (w *WAL) Rotate() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Close current file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close current WAL: %w", err)
	}

	// Archive old WAL with timestamp
	timestamp := time.Now().Unix()
	archivePath := fmt.Sprintf("%s.%d.old", w.path, timestamp)
	if err := os.Rename(w.path, archivePath); err != nil {
		return fmt.Errorf("failed to archive WAL: %w", err)
	}

	// Create new WAL file
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL: %w", err)
	}

	w.file = file
	return nil
}

// encodeEntry encodes an entry into bytes
// Format: [CRC32(4)][Timestamp(8)][KeySize(4)][ValueSize(4)][Key][Value][Type(1)]
func encodeEntry(entry Entry) ([]byte, error) {
	keySize := len(entry.Key)
	valueSize := len(entry.Value)

	// Calculate total size
	// 4 (CRC) + 8 (timestamp) + 4 (key size) + 4 (value size) + key + value + 1 (type)
	totalSize := 4 + 8 + 4 + 4 + keySize + valueSize + 1
	buf := make([]byte, totalSize)

	offset := 4 // Skip CRC for now

	// Write timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(entry.Timestamp))
	offset += 8

	// Write key size
	binary.BigEndian.PutUint32(buf[offset:], uint32(keySize))
	offset += 4

	// Write value size
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueSize))
	offset += 4

	// Write key
	copy(buf[offset:], entry.Key)
	offset += keySize

	// Write value
	copy(buf[offset:], entry.Value)
	offset += valueSize

	// Write type
	buf[offset] = byte(entry.Type)

	// Calculate and write CRC32 (over everything except CRC itself)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// readEntry reads a single entry from the reader
func readEntry(r io.Reader) (Entry, error) {
	// Read header: CRC(4) + Timestamp(8) + KeySize(4) + ValueSize(4)
	header := make([]byte, 20)
	if _, err := io.ReadFull(r, header); err != nil {
		return Entry{}, err
	}

	// Extract CRC
	expectedCRC := binary.BigEndian.Uint32(header[0:4])

	// Extract timestamp
	timestamp := int64(binary.BigEndian.Uint64(header[4:12]))

	// Extract sizes
	keySize := binary.BigEndian.Uint32(header[12:16])
	valueSize := binary.BigEndian.Uint32(header[16:20])

	// Read key, value, and type
	dataSize := keySize + valueSize + 1
	data := make([]byte, dataSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return Entry{}, err
	}

	// Verify CRC
	crcData := append(header[4:], data...)
	actualCRC := crc32.ChecksumIEEE(crcData)
	if actualCRC != expectedCRC {
		return Entry{}, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, actualCRC)
	}

	// Parse entry
	entry := Entry{
		Timestamp: timestamp,
		Key:       data[0:keySize],
		Value:     data[keySize : keySize+valueSize],
		Type:      OpType(data[keySize+valueSize]),
	}

	return entry, nil
}
