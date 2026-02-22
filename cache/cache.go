package cache

import (
	"context"
	"time"
)

// Cache defines the interface for cache backends
type Cache interface {
	// Get retrieves a value from the cache
	// Returns ErrCacheMiss if the key is not found
	Get(ctx context.Context, key string, dest interface{}) error

	// Set stores a value in the cache with an optional TTL
	// If ttl is 0, the item has no expiration
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error

	// Delete removes a value from the cache
	Delete(ctx context.Context, key string) error

	// Invalidate removes all keys matching a pattern
	// Pattern syntax depends on the backend implementation
	// For example: "user:*" matches all keys starting with "user:"
	Invalidate(ctx context.Context, pattern string) error

	// Clear removes all items from the cache
	Clear(ctx context.Context) error

	// Close closes the cache connection and releases resources
	Close() error

	// Name returns the name of the cache backend
	Name() string
}

// Stats provides cache statistics
type Stats struct {
	Hits     int64
	Misses   int64
	Sets     int64
	Deletes  int64
	Evictions int64
}

// StatsProvider is an optional interface for cache backends that provide statistics
type StatsProvider interface {
	Stats() Stats
	ResetStats()
}

// QueryCache wraps a Cache to provide query-specific caching
type QueryCache struct {
	cache      Cache
	defaultTTL time.Duration
	keyGen     KeyGenerator
	stats      Stats
}

// KeyGenerator generates cache keys for queries
type KeyGenerator func(query string, args ...interface{}) string

// DefaultKeyGenerator is the default key generator
// It creates a key by hashing the query and arguments
func DefaultKeyGenerator(query string, args ...interface{}) string {
	// Simple key generation - in production, use a proper hash function
	key := query
	for _, arg := range args {
		key += "/" + string(arg.(string))
	}
	return key
}

// NewQueryCache creates a new query cache
func NewQueryCache(cache Cache, defaultTTL time.Duration) *QueryCache {
	if defaultTTL == 0 {
		defaultTTL = 5 * time.Minute
	}
	return &QueryCache{
		cache:      cache,
		defaultTTL: defaultTTL,
		keyGen:     DefaultKeyGenerator,
	}
}

// SetKeyGenerator sets a custom key generator
func (qc *QueryCache) SetKeyGenerator(keyGen KeyGenerator) {
	qc.keyGen = keyGen
}

// GetOrExec retrieves a cached result or executes the query and caches the result
func (qc *QueryCache) GetOrExec(ctx context.Context, query string, args []interface{}, exec func() (interface{}, error)) (interface{}, error) {
	key := qc.keyGen(query, args...)

	// Try to get from cache
	var result interface{}
	err := qc.cache.Get(ctx, key, &result)
	if err == nil {
		qc.stats.Hits++
		return result, nil
	}

	qc.stats.Misses++

	// Execute the query
	result, err = exec()
	if err != nil {
		return nil, err
	}

	// Cache the result
	if err := qc.cache.Set(ctx, key, result, qc.defaultTTL); err != nil {
		// Log error but don't fail the query
		// In production, you might want to log this
	}
	qc.stats.Sets++

	return result, nil
}

// InvalidateQuery invalidates a cached query
func (qc *QueryCache) InvalidateQuery(ctx context.Context, query string, args ...interface{}) error {
	key := qc.keyGen(query, args...)
	return qc.cache.Delete(ctx, key)
}

// InvalidateTable invalidates all queries for a table
func (qc *QueryCache) InvalidateTable(ctx context.Context, table string) error {
	pattern := table + ":*"
	return qc.cache.Invalidate(ctx, pattern)
}

// Stats returns the cache statistics
func (qc *QueryCache) Stats() Stats {
	return qc.stats
}

// ResetStats resets the cache statistics
func (qc *QueryCache) ResetStats() {
	qc.stats = Stats{}
}

// Close closes the underlying cache
func (qc *QueryCache) Close() error {
	return qc.cache.Close()
}

// Errors
var (
	// ErrCacheMiss is returned when a key is not found in the cache
	ErrCacheMiss = &cacheError{msg: "cache miss"}

	// ErrCacheClosed is returned when operating on a closed cache
	ErrCacheClosed = &cacheError{msg: "cache closed"}

	// ErrInvalidType is returned when the cached value type doesn't match the destination
	ErrInvalidType = &cacheError{msg: "invalid type"}
)

type cacheError struct {
	msg string
}

func (e *cacheError) Error() string {
	return e.msg
}

// IsCacheMiss returns true if the error is a cache miss
func IsCacheMiss(err error) bool {
	return err == ErrCacheMiss
}

// IsCacheClosed returns true if the error indicates the cache is closed
func IsCacheClosed(err error) bool {
	return err == ErrCacheClosed
}
