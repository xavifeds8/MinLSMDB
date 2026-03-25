# MiniLSM - A Learning-Focused LSM Database

A from-scratch implementation of an LSM-tree (Log-Structured Merge-Tree) database for educational purposes. This project demonstrates core database concepts used in production systems like HBase, Cassandra, RocksDB, and LevelDB.

## Project Status

### ✅ Phase 1: Foundation (In Progress)

#### 1.1 Write-Ahead Log (WAL) - COMPLETED ✓

**Features Implemented:**
- ✅ Sync-per-write durability (fsync after every write)
- ✅ CRC32 checksums for corruption detection
- ✅ WAL replay for crash recovery
- ✅ WAL rotation for archiving old logs
- ✅ Support for PUT and DELETE operations
- ✅ Thread-safe concurrent writes
- ✅ Comprehensive test suite

**File Format:**
```
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

#### Next Steps:
- [ ] 1.2: MemTable - In-memory sorted structure
- [ ] 1.3: SSTable Writer - Flush to disk
- [ ] 1.4: Simple Get Operation

### 📋 Phase 2: Optimization (Planned)
- Bloom filters for efficient lookups
- Block-based storage with compression
- Sparse indexing for SSTables
- Manifest file for metadata

### 📋 Phase 3: Compaction (Planned)
- Size-tiered compaction strategy
- Background compaction threads
- Multi-level structure (L0-LN)

### 📋 Phase 4: Advanced Features (Planned)
- Range scans with merge iterators
- MVCC snapshots
- Enhanced crash recovery
- Metrics and monitoring

## Architecture Overview

```
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
)

func main() {
    // Create WAL
    w, err := wal.New("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer w.Close()

    // Write entries
    w.Append([]byte("key1"), []byte("value1"), wal.OpPut)
    w.Append([]byte("key2"), []byte("value2"), wal.OpPut)
    w.Append([]byte("key1"), nil, wal.OpDelete)

    // Replay for recovery
    entries, err := w.Replay()
    if err != nil {
        log.Fatal(err)
    }

    // Process entries...
    for _, entry := range entries {
        // Apply to MemTable
    }
}
```

## Project Structure

```
minilsm/
├── wal/              # Write-Ahead Log implementation
│   ├── wal.go        # Core WAL logic
│   └── wal_test.go   # Comprehensive tests
├── memtable/         # In-memory sorted structure (TODO)
├── sstable/          # SSTable reader/writer (TODO)
├── db/               # Main DB interface (TODO)
├── util/             # Utilities (bloom filters, etc.) (TODO)
├── cmd/
│   └── minilsm/      # Demo application
│       └── main.go
├── go.mod
└── README.md
```

## Testing

The WAL module includes comprehensive tests covering:

- ✅ Basic append and replay operations
- ✅ Crash recovery scenarios
- ✅ WAL rotation
- ✅ Delete operations
- ✅ Large values (1MB+)
- ✅ Corruption detection
- ✅ Concurrent writes (1000 operations across 10 goroutines)

Run tests:
```bash
go test -v ./wal
```

Run with race detector:
```bash
go test -race ./wal
```

## Learning Resources

### Key Concepts Covered

1. **Write-Ahead Logging (WAL)**
   - Durability guarantees
   - fsync and disk persistence
   - Recovery mechanisms

2. **Data Integrity**
   - CRC32 checksums
   - Corruption detection
   - Graceful degradation

3. **Concurrency**
   - Mutex-based synchronization
   - Thread-safe operations
   - Race condition prevention

### Recommended Reading

- **Papers:**
  - "The Log-Structured Merge-Tree (LSM-Tree)" - O'Neil et al., 1996
  - "Bigtable: A Distributed Storage System" - Google, 2006
  - "Cassandra: A Decentralized Structured Storage System" - Facebook, 2009

- **Books:**
  - "Database Internals" by Alex Petrov
  - "Designing Data-Intensive Applications" by Martin Kleppmann

- **Open Source:**
  - RocksDB: https://github.com/facebook/rocksdb
  - LevelDB: https://github.com/google/leveldb
  - BadgerDB: https://github.com/dgraph-io/badger

## Performance Considerations

### Current Implementation (Sync-per-write)

**Pros:**
- ✅ Maximum durability - no data loss
- ✅ Simple implementation
- ✅ Easy to reason about

**Cons:**
- ❌ Low throughput (~200 writes/sec)
- ❌ High latency per write (~5ms)
- ❌ Disk I/O bottleneck

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
- [ ] Implement MemTable with skip list or red-black tree
- [ ] Add SSTable writer with block-based format
- [ ] Implement basic Get operation
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

**Built for learning. Optimized for understanding. 🚀**
