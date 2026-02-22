package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// MemoryCache is an in-memory cache implementation
type MemoryCache struct {
	mu    sync.RWMutex
	items map[string]*cacheItem
	stats Stats
	closed bool
}

type cacheItem struct {
	value      []byte
	expiration *time.Time
	createdAt  time.Time
}

// NewMemoryCache creates a new in-memory cache
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		items: make(map[string]*cacheItem),
	}
}

// Get retrieves a value from the cache
func (m *MemoryCache) Get(ctx context.Context, key string, dest interface{}) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return ErrCacheClosed
	}

	item, ok := m.items[key]
	if !ok {
		m.stats.Misses++
		return ErrCacheMiss
	}

	// Check expiration
	if item.expiration != nil && time.Now().After(*item.expiration) {
		m.stats.Misses++
		delete(m.items, key)
		return ErrCacheMiss
	}

	m.stats.Hits++

	// Deserialize the value
	return json.Unmarshal(item.value, dest)
}

// Set stores a value in the cache
func (m *MemoryCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrCacheClosed
	}

	// Serialize the value
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	item := &cacheItem{
		value:     data,
		createdAt: time.Now(),
	}

	if ttl > 0 {
		expiration := time.Now().Add(ttl)
		item.expiration = &expiration
	}

	m.items[key] = item
	m.stats.Sets++

	return nil
}

// Delete removes a value from the cache
func (m *MemoryCache) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrCacheClosed
	}

	if _, ok := m.items[key]; ok {
		delete(m.items, key)
		m.stats.Deletes++
	}

	return nil
}

// Invalidate removes all keys matching a pattern
// Pattern syntax:
//   - "user:*" matches all keys starting with "user:"
//   - "*:123" matches all keys ending with ":123"
//   - "*" matches all keys
func (m *MemoryCache) Invalidate(ctx context.Context, pattern string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrCacheClosed
	}

	if pattern == "*" {
		m.items = make(map[string]*cacheItem)
		return nil
	}

	// Simple pattern matching
	// For better performance with large caches, consider using a more efficient algorithm
	for key := range m.items {
		if matchPattern(key, pattern) {
			delete(m.items, key)
			m.stats.Deletes++
		}
	}

	return nil
}

// Clear removes all items from the cache
func (m *MemoryCache) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrCacheClosed
	}

	m.items = make(map[string]*cacheItem)
	return nil
}

// Close closes the cache
func (m *MemoryCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrCacheClosed
	}

	m.items = nil
	m.closed = true
	return nil
}

// Name returns the name of the cache backend
func (m *MemoryCache) Name() string {
	return "memory"
}

// Stats returns the cache statistics
func (m *MemoryCache) Stats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.stats
}

// ResetStats resets the cache statistics
func (m *MemoryCache) ResetStats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stats = Stats{}
}

// Count returns the number of items in the cache
func (m *MemoryCache) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.items)
}

// cleanupExpired removes expired items from the cache
// This should be called periodically to prevent memory leaks
func (m *MemoryCache) cleanupExpired() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for key, item := range m.items {
		if item.expiration != nil && now.After(*item.expiration) {
			delete(m.items, key)
			m.stats.Evictions++
		}
	}
}

// StartCleanup starts a background goroutine that periodically cleans up expired items
func (m *MemoryCache) StartCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			m.cleanupExpired()
		}
	}()
}

// matchPattern checks if a key matches a pattern
func matchPattern(key, pattern string) bool {
	if pattern == "*" {
		return true
	}

	if len(pattern) == 0 {
		return key == ""
	}

	// Simple wildcard matching
	// "prefix:*" matches keys starting with "prefix:"
	// "*:suffix" matches keys ending with ":suffix"
	if pattern[0] == '*' && pattern[len(pattern)-1] != '*' {
		suffix := pattern[1:]
		return len(key) >= len(suffix) && key[len(key)-len(suffix):] == suffix
	}

	if pattern[len(pattern)-1] == '*' && pattern[0] != '*' {
		prefix := pattern[:len(pattern)-1]
		return len(key) >= len(prefix) && key[:len(prefix)] == prefix
	}

	// Exact match
	return key == pattern
}
