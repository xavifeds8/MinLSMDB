package util

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure
// used to test whether an element is a member of a set
type BloomFilter struct {
	bits       []byte  // Bit array
	numBits    uint64  // Total number of bits
	numHashes  uint32  // Number of hash functions
	numEntries uint64  // Number of entries added (for statistics)
}

// NewBloomFilter creates a new bloom filter
// numEntries: expected number of entries
// falsePositiveRate: desired false positive rate (e.g., 0.01 for 1%)
func NewBloomFilter(numEntries uint64, falsePositiveRate float64) *BloomFilter {
	if numEntries == 0 {
		numEntries = 1000 // Default
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		falsePositiveRate = 0.01 // Default 1%
	}

	// Calculate optimal number of bits: m = -n*ln(p) / (ln(2)^2)
	numBits := uint64(math.Ceil(-float64(numEntries) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)))
	
	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	numHashes := uint32(math.Ceil((float64(numBits) / float64(numEntries)) * math.Ln2))
	
	// Ensure at least 1 hash function
	if numHashes == 0 {
		numHashes = 1
	}

	// Calculate number of bytes needed
	numBytes := (numBits + 7) / 8

	return &BloomFilter{
		bits:      make([]byte, numBytes),
		numBits:   numBits,
		numHashes: numHashes,
	}
}

// Add adds a key to the bloom filter
func (bf *BloomFilter) Add(key []byte) {
	hash1, hash2 := bf.hash(key)
	
	for i := uint32(0); i < bf.numHashes; i++ {
		// Double hashing: hash_i(x) = hash1(x) + i * hash2(x)
		combinedHash := hash1 + uint64(i)*hash2
		bitPos := combinedHash % bf.numBits
		
		// Set the bit
		byteIndex := bitPos / 8
		bitIndex := bitPos % 8
		bf.bits[byteIndex] |= (1 << bitIndex)
	}
	
	bf.numEntries++
}

// MayContain checks if a key might be in the set
// Returns true if the key might be present (with false positive rate)
// Returns false if the key is definitely not present
func (bf *BloomFilter) MayContain(key []byte) bool {
	hash1, hash2 := bf.hash(key)
	
	for i := uint32(0); i < bf.numHashes; i++ {
		combinedHash := hash1 + uint64(i)*hash2
		bitPos := combinedHash % bf.numBits
		
		// Check the bit
		byteIndex := bitPos / 8
		bitIndex := bitPos % 8
		if (bf.bits[byteIndex] & (1 << bitIndex)) == 0 {
			return false // Definitely not present
		}
	}
	
	return true // Might be present
}

// hash computes two hash values for double hashing
func (bf *BloomFilter) hash(key []byte) (uint64, uint64) {
	// Use FNV-1a hash
	h := fnv.New64a()
	h.Write(key)
	hash1 := h.Sum64()
	
	// Second hash: use FNV-1a with a different seed (add key length)
	h.Reset()
	h.Write(key)
	h.Write([]byte{byte(len(key))})
	hash2 := h.Sum64()
	
	// Ensure hash2 is odd (for better distribution)
	if hash2%2 == 0 {
		hash2++
	}
	
	return hash1, hash2
}

// Serialize converts the bloom filter to bytes for storage
func (bf *BloomFilter) Serialize() []byte {
	// Format: [numBits(8)][numHashes(4)][numEntries(8)][bits...]
	headerSize := 8 + 4 + 8
	result := make([]byte, headerSize+len(bf.bits))
	
	offset := 0
	binary.BigEndian.PutUint64(result[offset:], bf.numBits)
	offset += 8
	binary.BigEndian.PutUint32(result[offset:], bf.numHashes)
	offset += 4
	binary.BigEndian.PutUint64(result[offset:], bf.numEntries)
	offset += 8
	copy(result[offset:], bf.bits)
	
	return result
}

// Deserialize creates a bloom filter from serialized bytes
func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 20 {
		return nil, nil // No bloom filter data
	}
	
	offset := 0
	numBits := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	numHashes := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	numEntries := binary.BigEndian.Uint64(data[offset:])
	offset += 8
	
	bits := make([]byte, len(data)-offset)
	copy(bits, data[offset:])
	
	return &BloomFilter{
		bits:       bits,
		numBits:    numBits,
		numHashes:  numHashes,
		numEntries: numEntries,
	}, nil
}

// Size returns the size of the bloom filter in bytes
func (bf *BloomFilter) Size() int {
	return len(bf.bits)
}

// NumEntries returns the number of entries added to the filter
func (bf *BloomFilter) NumEntries() uint64 {
	return bf.numEntries
}

// EstimatedFalsePositiveRate estimates the current false positive rate
func (bf *BloomFilter) EstimatedFalsePositiveRate() float64 {
	if bf.numEntries == 0 {
		return 0
	}
	
	// FPR ≈ (1 - e^(-k*n/m))^k
	// where k = numHashes, n = numEntries, m = numBits
	exponent := -float64(bf.numHashes) * float64(bf.numEntries) / float64(bf.numBits)
	return math.Pow(1-math.Exp(exponent), float64(bf.numHashes))
}
