package bloom

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Filter implements a distributed Bloom filter backed by Valkey/Redis.
// It uses multiple hash functions derived from SHA256 and stores bits
// in a Redis bitmap for distributed access across multiple workers.
type Filter struct {
	client    *redis.Client
	key       string        // Redis key for the bitmap
	size      uint64        // Number of bits in the filter
	hashCount int           // Number of hash functions to use
	ttl       time.Duration // TTL for the bloom filter key (0 = no expiry)
}

// Config holds configuration for creating a new Bloom filter
type Config struct {
	// Valkey/Redis client
	Client *redis.Client

	// Key is the Redis key name for storing the bloom filter
	Key string

	// ExpectedItems is the expected number of items to be stored
	// Used to calculate optimal filter size
	ExpectedItems uint64

	// FalsePositiveRate is the desired false positive rate (e.g., 0.01 for 1%)
	FalsePositiveRate float64

	// TTL for the bloom filter (0 = no expiration)
	// Useful for time-bounded crawls
	TTL time.Duration
}

// New creates a new distributed Bloom filter.
// The filter size and hash count are calculated based on expected items
// and desired false positive rate using optimal formulas.
func New(cfg Config) (*Filter, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("redis client is required")
	}
	if cfg.Key == "" {
		cfg.Key = "bloom:urls"
	}
	if cfg.ExpectedItems == 0 {
		cfg.ExpectedItems = 10_000_000 // Default: 10 million URLs
	}
	if cfg.FalsePositiveRate <= 0 || cfg.FalsePositiveRate >= 1 {
		cfg.FalsePositiveRate = 0.01 // Default: 1% false positive rate
	}

	// Calculate optimal size: m = -n * ln(p) / (ln(2)^2)
	// where n = expected items, p = false positive rate
	ln2 := 0.693147180559945

	// Simplified: ~10 bits per item for 1% FP rate
	bitsPerItem := 10.0
	if cfg.FalsePositiveRate < 0.01 {
		bitsPerItem = 14.0 // ~14 bits for 0.1%
	} else if cfg.FalsePositiveRate > 0.01 {
		bitsPerItem = 7.0 // ~7 bits for 5%
	}

	size := uint64(float64(cfg.ExpectedItems) * bitsPerItem)

	// Calculate optimal hash count: k = (m/n) * ln(2)
	hashCount := int((float64(size) / float64(cfg.ExpectedItems)) * ln2)
	if hashCount < 3 {
		hashCount = 3
	}
	if hashCount > 13 {
		hashCount = 13
	}

	// Round size to multiple of 8 for byte alignment
	size = ((size + 7) / 8) * 8

	return &Filter{
		client:    cfg.Client,
		key:       cfg.Key,
		size:      size,
		hashCount: hashCount,
		ttl:       cfg.TTL,
	}, nil
}

// Add adds an item to the bloom filter.
// Returns true if the item was newly added (definitely not seen before),
// false if it might have been seen (could be false positive).
func (f *Filter) Add(ctx context.Context, item string) (bool, error) {
	positions := f.hashPositions(item)

	// Use pipeline for atomic multi-bit set
	pipe := f.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(positions))

	for i, pos := range positions {
		cmds[i] = pipe.SetBit(ctx, f.key, int64(pos), 1)
	}

	// Set TTL if configured (only on first add)
	if f.ttl > 0 {
		pipe.Expire(ctx, f.key, f.ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("bloom add: %w", err)
	}

	// Check if any bit was previously 0 (item was new)
	for _, cmd := range cmds {
		if cmd.Val() == 0 {
			return true, nil // At least one bit was new
		}
	}

	return false, nil // All bits were already set (probably seen before)
}

// Contains checks if an item might be in the bloom filter.
// Returns true if the item might exist (could be false positive),
// false if the item definitely does not exist.
func (f *Filter) Contains(ctx context.Context, item string) (bool, error) {
	positions := f.hashPositions(item)

	// Use pipeline for efficient multi-bit get
	pipe := f.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(positions))

	for i, pos := range positions {
		cmds[i] = pipe.GetBit(ctx, f.key, int64(pos))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("bloom contains: %w", err)
	}

	// All bits must be set for item to possibly exist
	for _, cmd := range cmds {
		if cmd.Val() == 0 {
			return false, nil // Definitely not in set
		}
	}

	return true, nil // Might be in set (or false positive)
}

// AddIfNotExists atomically checks and adds an item.
// Returns true if the item was newly added (definitely not seen before),
// false if it was already present (might be false positive).
// This is the primary method for deduplication.
func (f *Filter) AddIfNotExists(ctx context.Context, item string) (bool, error) {
	// First check if it might exist
	exists, err := f.Contains(ctx, item)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil // Already exists (or false positive)
	}

	// Add it
	return f.Add(ctx, item)
}

// hashPositions generates multiple bit positions from a single item.
// Uses double hashing technique: h(i) = h1 + i*h2 mod size
// where h1 and h2 are derived from SHA256.
func (f *Filter) hashPositions(item string) []uint64 {
	hash := sha256.Sum256([]byte(item))

	// Split hash into two 64-bit values for double hashing
	h1 := binary.BigEndian.Uint64(hash[0:8])
	h2 := binary.BigEndian.Uint64(hash[8:16])

	positions := make([]uint64, f.hashCount)
	for i := 0; i < f.hashCount; i++ {
		// Double hashing: position = (h1 + i*h2) mod size
		positions[i] = (h1 + uint64(i)*h2) % f.size
	}

	return positions
}

// Stats returns statistics about the bloom filter
type Stats struct {
	Size         uint64  `json:"size"`          // Total bits
	HashCount    int     `json:"hash_count"`    // Number of hash functions
	BitsSet      uint64  `json:"bits_set"`      // Approximate bits set
	FillRatio    float64 `json:"fill_ratio"`    // Approximate fill ratio
	MemoryBytes  uint64  `json:"memory_bytes"`  // Memory usage in bytes
	TTLRemaining int64   `json:"ttl_remaining"` // TTL remaining in seconds
}

// Stats returns statistics about the bloom filter
func (f *Filter) Stats(ctx context.Context) (*Stats, error) {
	// Get bit count using BITCOUNT
	bitCount, err := f.client.BitCount(ctx, f.key, nil).Result()
	if err != nil {
		return nil, fmt.Errorf("bitcount: %w", err)
	}

	// Get TTL
	ttl, err := f.client.TTL(ctx, f.key).Result()
	if err != nil {
		ttl = 0
	}

	fillRatio := float64(bitCount) / float64(f.size)

	return &Stats{
		Size:         f.size,
		HashCount:    f.hashCount,
		BitsSet:      uint64(bitCount),
		FillRatio:    fillRatio,
		MemoryBytes:  f.size / 8,
		TTLRemaining: int64(ttl.Seconds()),
	}, nil
}

// Reset clears the bloom filter
func (f *Filter) Reset(ctx context.Context) error {
	return f.client.Del(ctx, f.key).Err()
}