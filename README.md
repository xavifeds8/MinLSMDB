# MiniLSM - A Learning-Focused LSM Database

A from-scratch implementation of an LSM-tree (Log-Structured Merge-Tree) database for educational purposes. This project demonstrates core database concepts used in production systems like HBase, Cassandra, RocksDB, and LevelDB.

## Project Status

### Phase 1: Foundation (In Progress)

#### 1.1 Write-Ahead Log (WAL) - COMPLETED

**Features Implemented:**

- Sync-per-write durability (fsync after every write)
- CRC32 checksums for corruption detection
- WAL replay for crash recovery
- WAL rotation for archiving old logs
- Support for PUT and DELETE operations
- Thread-safe concurrent writes
- Comprehensive test suite

**File Format:**

```text
Entry Structure:
┌─────────────┬──────────┬────────────┬────────────┬─────┬───────┬─────────┐
│   CRC32     │ Timestamp│  Key Size  │ Value Size │ Key │ Value │  Type   │
│  (4 bytes)  │ (8 bytes)│  (4 bytes) │  (4 bytes) │ (N) │  (M)  │(1 byte) │
└─────────────┴──────────┴────────────┴────────────┴─────┴───────┴─────────┘
```

**Key Design Decisions:**

- **Sync-per-write**: Maximum durability - no data loss after acknowledgment
- **CRC32 validation**: Detects corruption during replay
- **Graceful degradation**: Stops replay at first corrupted entry
- **Mutex protection**: Thread-safe for concurrent writes

**Performance Characteristics:**

- Write latency: ~5ms per operation (dominated by fsync)
- Throughput: ~200 writes/second (single-threaded)
- Recovery time: O(n) where n = number of entries

#### 1.2 MemTable - COMPLETED

**Features Implemented:**

- Skip list data structure for O(log n) operations
- Thread-safe with RWMutex (multiple readers, single writer)
- Size tracking with configurable threshold
- Tombstone markers for deletions
- Iterator support for sorted traversal
- Crash recovery via WAL replay
- Comprehensive test suite with concurrency tests

**Data Structure:**

- **Skip List**: Probabilistic balanced structure
- **Max Level**: 16 levels
- **Probability**: 0.5 for level promotion
- **Average Complexity**: O(log n) for insert/search/delete

**Key Design Decisions:**

- **RWMutex**: Allows concurrent reads without blocking
- **Tombstones**: Deletions marked with special value (0xFF 0xFF 0xFF 0xFF)
- **Size tracking**: Monitors memory usage for flush decisions
- **Sorted order**: Maintains keys in lexicographic order

**Performance Characteristics:**

- Put operation: ~1-2 microseconds (O(log n))
- Get operation: ~1-2 microseconds (O(log n))
- Memory overhead: ~32 bytes per entry (estimated)
- Concurrent reads: No contention between readers

#### Next Steps

- [ ] 1.3: SSTable Writer - Flush to disk
- [ ] 1.4: SSTable Reader - Read from disk
- [ ] 1.5: DB Interface - Tie everything together

### Phase 2: Optimization (Planned)

- Bloom filters for efficient lookups
- Block-based storage with compression
- Sparse indexing for SSTables
- Manifest file for metadata

### Phase 3: Compaction (Planned)

- Size-tiered compaction strategy
- Background compaction threads
- Multi-level structure (L0-LN)

### Phase 4: Advanced Features (Planned)

- Range scans with merge iterators
- MVCC snapshots
- Enhanced crash recovery
- Metrics and monitoring

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                         Client API                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Write Path                             │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐         │
│  │   WAL    │ ───▶ │ MemTable │ ───▶ │ SSTable  │         │
│  │ (Append) │      │ (Sorted) │      │  (Disk)  │         │
│  └──────────┘      └──────────┘      └──────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Read Path                              │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐         │
│  │ MemTable │ ───▶ │ SSTable  │ ───▶ │ SSTable  │         │
│  │ (Check)  │      │   (L0)   │      │  (L1-N)  │         │
│  └──────────┘      └──────────┘      └──────────┘         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Background Tasks                         │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐         │
│  │  Flush   │      │Compaction│      │  Cleanup │         │
│  │  Thread  │      │  Thread  │      │  Thread  │         │
│  └──────────┘      └──────────┘      └──────────┘         │
└─────────────────────────────────────────────────────────────┘
```

## Getting Started

### Prerequisites

- Go 1.18 or higher

### Installation

```bash
# Clone or navigate to the project
cd minilsm

# Run tests
go test -v ./wal

# Run demo
go run cmd/minilsm/main.go
```

### Basic Usage

```go
package main

import (
    "log"
    "minilsm/wal"
    "minilsm/memtable"
)

func main() {
    // Create WAL
    w, err := wal.New("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer w.Close()

    // Create MemTable (4MB max size)
    mt := memtable.New(4 * 1024 * 1024)

    // Write entries (WAL first for durability, then MemTable)
    key := []byte("key1")
    value := []byte("value1")
    
    w.Append(key, value, wal.OpPut)
    mt.Put(key, value)

    // Read from MemTable
    val, found, err := mt.Get(key)
    if err != nil {
        log.Fatal(err)
    }
    if found {
        log.Printf("Found: %s = %s", key, val)
    }

    // Crash recovery: Replay WAL to rebuild MemTable
    entries, err := w.Replay()
    if err != nil {
        log.Fatal(err)
    }

    mt = memtable.New(4 * 1024 * 1024)
    for _, entry := range entries {
        if entry.Type == wal.OpPut {
            mt.Put(entry.Key, entry.Value)
        } else {
            mt.Delete(entry.Key)
        }
    }
}
```

## Project Structure

```text
minilsm/
├── wal/              # Write-Ahead Log implementation
│   ├── wal.go        # Core WAL logic
│   └── wal_test.go   # Comprehensive tests
├── memtable/         # In-memory sorted structure
│   ├── memtable.go   # MemTable implementation
│   └── memtable_test.go  # Comprehensive tests
├── util/             # Utility data structures
│   └── skiplist.go   # Skip list implementation
├── sstable/          # SSTable reader/writer (TODO)
├── db/               # Main DB interface (TODO)
├── cmd/
│   └── minilsm/      # Demo application
│       └── main.go
├── go.mod
└── README.md
```

## Testing

### WAL Tests

The WAL module includes comprehensive tests covering:

- Basic append and replay operations
- Crash recovery scenarios
- WAL rotation
- Delete operations
- Large values (1MB+)
- Corruption detection
- Concurrent writes (1000 operations across 10 goroutines)

### MemTable Tests

The MemTable module includes comprehensive tests covering:

- Basic Put/Get/Delete operations
- Key updates and overwrites
- Tombstone handling
- Size tracking accuracy
- Iterator functionality and seeking
- Concurrent reads (10 goroutines, 100 ops each)
- Concurrent writes (10 goroutines, 100 ops each)
- Mixed concurrent operations
- Large values (1MB+)
- Edge cases (empty keys, clear operations)

### Running Tests

Run all tests:

```bash
go test -v ./...
```

Run specific module:

```bash
go test -v ./wal
go test -v ./memtable
```

Run with race detector:

```bash
go test -race ./...
```

Run benchmarks:

```bash
go test -bench=. ./memtable
```

## Learning Resources

### Key Concepts Covered

1. **Write-Ahead Logging (WAL)**
   - Durability guarantees
   - fsync and disk persistence
   - Recovery mechanisms

2. **MemTable & Skip Lists**
   - Probabilistic data structures
   - O(log n) operations
   - In-memory sorted storage
   - Tombstone deletions

3. **Data Integrity**
   - CRC32 checksums
   - Corruption detection
   - Graceful degradation

4. **Concurrency**
   - RWMutex for reader/writer locks
   - Thread-safe operations
   - Race condition prevention
   - Concurrent read optimization

5. **Crash Recovery**
   - WAL replay mechanism
   - State reconstruction
   - Durability guarantees

### Recommended Reading

**Papers:**

- "The Log-Structured Merge-Tree (LSM-Tree)" - O'Neil et al., 1996
- "Bigtable: A Distributed Storage System" - Google, 2006
- "Cassandra: A Decentralized Structured Storage System" - Facebook, 2009

**Books:**

- "Database Internals" by Alex Petrov
- "Designing Data-Intensive Applications" by Martin Kleppmann

**Open Source:**

- [RocksDB](https://github.com/facebook/rocksdb)
- [LevelDB](https://github.com/google/leveldb)
- [BadgerDB](https://github.com/dgraph-io/badger)

## Performance Considerations

### Current Implementation (Sync-per-write)

**Pros:**

- Maximum durability - no data loss
- Simple implementation
- Easy to reason about

**Cons:**

- Low throughput (~200 writes/sec)
- High latency per write (~5ms)
- Disk I/O bottleneck

### Future Optimizations

**Group Commit (Planned):**

- Batch N writes, single fsync
- Expected: ~600 writes/sec
- Trade-off: Slightly higher latency for individual writes

**Time-Based Batching (Planned):**

- Flush every T milliseconds
- Bounded latency guarantees
- Adaptive to workload

## Contributing

This is an educational project. Feel free to:

- Experiment with the code
- Add new features
- Optimize performance
- Improve documentation

## Future Enhancements

### Short Term

- [x] Implement MemTable with skip list
- [x] Add comprehensive test suite
- [ ] Add SSTable writer with block-based format
- [ ] Implement SSTable reader
- [ ] Create unified DB interface
- [ ] Add benchmarking suite

### Medium Term

- [ ] Bloom filters for read optimization
- [ ] Block compression (Snappy/LZ4)
- [ ] Size-tiered compaction
- [ ] Background flush/compaction threads

### Long Term

- [ ] MVCC with snapshots
- [ ] Range scan support
- [ ] Leveled compaction strategy
- [ ] Metrics and observability
- [ ] Learned index structures (research)

## License

This is an educational project for learning purposes.

## Acknowledgments

Inspired by production LSM-tree implementations:

- RocksDB (Facebook)
- LevelDB (Google)
- Cassandra (Apache)
- HBase (Apache)
- BadgerDB (Dgraph)

---

**Built for learning. Optimized for understanding.**
