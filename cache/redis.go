package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// RedisClient is the interface for Redis client
// This allows us to work with different Redis client libraries
type RedisClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	Del(ctx context.Context, keys ...string) error
	Keys(ctx context.Context, pattern string) ([]string, error)
	FlushDB(ctx context.Context) error
	Close() error
}

// RedisCache is a Redis-backed cache implementation
type RedisCache struct {
	client RedisClient
	stats  Stats
	closed bool
	prefix string // Optional prefix for all keys
}

// NewRedisCache creates a new Redis cache
func NewRedisCache(client RedisClient) *RedisCache {
	return &RedisCache{
		client: client,
		prefix: "",
	}
}

// NewRedisCacheWithPrefix creates a new Redis cache with a key prefix
func NewRedisCacheWithPrefix(client RedisClient, prefix string) *RedisCache {
	return &RedisCache{
		client: client,
		prefix: prefix,
	}
}

// Get retrieves a value from the cache
func (r *RedisCache) Get(ctx context.Context, key string, dest interface{}) error {
	if r.closed {
		return ErrCacheClosed
	}

	fullKey := r.prefix + key

	val, err := r.client.Get(ctx, fullKey)
	if err != nil {
		r.stats.Misses++
		if err == ErrCacheMiss {
			return ErrCacheMiss
		}
		return fmt.Errorf("redis get failed: %w", err)
	}

	r.stats.Hits++

	// Deserialize the value
	if err := json.Unmarshal([]byte(val), dest); err != nil {
		return fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return nil
}

// Set stores a value in the cache
func (r *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if r.closed {
		return ErrCacheClosed
	}

	fullKey := r.prefix + key

	// Serialize the value
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	if err := r.client.Set(ctx, fullKey, data, ttl); err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}

	r.stats.Sets++
	return nil
}

// Delete removes a value from the cache
func (r *RedisCache) Delete(ctx context.Context, key string) error {
	if r.closed {
		return ErrCacheClosed
	}

	fullKey := r.prefix + key

	if err := r.client.Del(ctx, fullKey); err != nil {
		return fmt.Errorf("redis del failed: %w", err)
	}

	r.stats.Deletes++
	return nil
}

// Invalidate removes all keys matching a pattern
func (r *RedisCache) Invalidate(ctx context.Context, pattern string) error {
	if r.closed {
		return ErrCacheClosed
	}

	fullPattern := r.prefix + pattern

	// Get all matching keys
	keys, err := r.client.Keys(ctx, fullPattern)
	if err != nil {
		return fmt.Errorf("redis keys failed: %w", err)
	}

	// Delete all matching keys
	if len(keys) > 0 {
		if err := r.client.Del(ctx, keys...); err != nil {
			return fmt.Errorf("redis del failed: %w", err)
		}
		r.stats.Deletes += int64(len(keys))
	}

	return nil
}

// Clear removes all items from the cache
func (r *RedisCache) Clear(ctx context.Context) error {
	if r.closed {
		return ErrCacheClosed
	}

	if err := r.client.FlushDB(ctx); err != nil {
		return fmt.Errorf("redis flushdb failed: %w", err)
	}

	return nil
}

// Close closes the Redis connection
func (r *RedisCache) Close() error {
	if r.closed {
		return ErrCacheClosed
	}

	r.closed = true
	return r.client.Close()
}

// Name returns the name of the cache backend
func (r *RedisCache) Name() string {
	return "redis"
}

// Stats returns the cache statistics
func (r *RedisCache) Stats() Stats {
	return r.stats
}

// ResetStats resets the cache statistics
func (r *RedisCache) ResetStats() {
	r.stats = Stats{}
}

// ============ Helper Functions ============

// RedisKeyGenerator generates Redis cache keys with a consistent format
func RedisKeyGenerator(prefix string) KeyGenerator {
	return func(query string, args ...interface{}) string {
		key := prefix + ":" + query
		for _, arg := range args {
			key += ":" + fmt.Sprintf("%v", arg)
		}
		return key
	}
}

// TableKeyGenerator generates cache keys for a specific table
// Useful for cache invalidation at the table level
func TableKeyGenerator(table string) KeyGenerator {
	return func(query string, args ...interface{}) string {
		// Create a key like "table:users:query:SELECT * FROM users WHERE id = 1"
		return fmt.Sprintf("table:%s:query:%s", table, query)
	}
}

// ModelKeyGenerator generates cache keys for a specific model
// Useful for cache invalidation at the model level
func ModelKeyGenerator(modelName string, modelID interface{}) KeyGenerator {
	return func(query string, args ...interface{}) string {
		return fmt.Sprintf("model:%s:id:%v", modelName, modelID)
	}
}
