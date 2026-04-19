package prepared

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/gomodul/db/cache"
)

// Statement represents a cached prepared statement
type Statement struct {
	Query  string
	Stmt   *sql.Stmt
	UsedAt time.Time
}

// StatementCache manages prepared statement caching
type StatementCache struct {
	mu    sync.RWMutex
	cache cache.Cache
	ttl   time.Duration
	db    *sql.DB
}

// Config holds configuration for the statement cache
type Config struct {
	// MaxEntries is the maximum number of cached statements (deprecated - use cache backend)
	MaxEntries int
	// TTL is the time-to-live for cached statements
	TTL time.Duration
	// Enable enables/disables the cache
	Enable bool
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		MaxEntries: 100,
		TTL:        10 * time.Minute,
		Enable:     true,
	}
}

// NewStatementCache creates a new prepared statement cache
func NewStatementCache(db *sql.DB, cfg *Config, backend cache.Cache) *StatementCache {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	if backend == nil {
		// Create a simple in-memory cache implementation
		backend = &simpleCache{
			items: make(map[string]*cacheItem),
		}
	}

	return &StatementCache{
		cache: backend,
		ttl:   cfg.TTL,
		db:    db,
	}
}

// Prepare gets or creates a prepared statement
func (sc *StatementCache) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	if sc == nil {
		return nil, fmt.Errorf("statement cache is not initialized")
	}

	// Try to get from cache
	key := cacheKey(query)
	var cachedStmt Statement
	err := sc.cache.Get(ctx, key, &cachedStmt)
	if err == nil {
		// Update usage time
		cachedStmt.UsedAt = time.Now()
		_ = sc.cache.Set(ctx, key, cachedStmt, sc.ttl)
		return cachedStmt.Stmt, nil
	}

	// Prepare new statement
	stmt, err := sc.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// Cache the statement
	cachedStmt = Statement{
		Query:  query,
		Stmt:   stmt,
		UsedAt: time.Now(),
	}
	_ = sc.cache.Set(ctx, key, cachedStmt, sc.ttl)

	return stmt, nil
}

// Invalidate removes a statement from cache
func (sc *StatementCache) Invalidate(ctx context.Context, query string) error {
	if sc == nil {
		return nil
	}
	return sc.cache.Delete(ctx, cacheKey(query))
}

// Clear removes all cached statements
func (sc *StatementCache) Clear(ctx context.Context) error {
	if sc == nil {
		return nil
	}
	return sc.cache.Clear(ctx)
}

// Close closes all cached statements and clears the cache
func (sc *StatementCache) Close() error {
	if sc == nil {
		return nil
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Close all cached statements
	if simpleCache, ok := sc.cache.(*simpleCache); ok {
		simpleCache.mu.Lock()
		defer simpleCache.mu.Unlock()
		for _, item := range simpleCache.items {
			if stmt, ok := item.value.(*Statement); ok && stmt.Stmt != nil {
				_ = stmt.Stmt.Close()
			}
		}
		simpleCache.items = make(map[string]*cacheItem)
	}

	return sc.cache.Close()
}

// Stats returns cache statistics if available
func (sc *StatementCache) Stats(ctx context.Context) map[string]interface{} {
	if sc == nil {
		return nil
	}

	stats := make(map[string]interface{})
	stats["type"] = "prepared_statement"
	stats["ttl"] = sc.ttl.String()

	// Try to get stats from backend if it supports it
	if statsProvider, ok := sc.cache.(cache.StatsProvider); ok {
		cacheStats := statsProvider.Stats()
		stats["hits"] = cacheStats.Hits
		stats["misses"] = cacheStats.Misses
		stats["sets"] = cacheStats.Sets
	}

	return stats
}

// CleanExpired removes expired statements from cache
func (sc *StatementCache) CleanExpired(ctx context.Context) {
	if sc == nil {
		return
	}

	if simpleCache, ok := sc.cache.(*simpleCache); ok {
		simpleCache.mu.Lock()
		defer simpleCache.mu.Unlock()

		now := time.Now()
		for key, item := range simpleCache.items {
			if stmt, ok := item.value.(*Statement); ok {
				if now.Sub(stmt.UsedAt) > sc.ttl {
					// Close the statement before removing
					if stmt.Stmt != nil {
						_ = stmt.Stmt.Close()
					}
					delete(simpleCache.items, key)
				}
			}
		}
	}
}

// cacheKey generates a cache key for a query
func cacheKey(query string) string {
	return fmt.Sprintf("stmt:%s", query)
}

// Manager manages multiple statement caches (one per database)
type Manager struct {
	mu     sync.RWMutex
	caches map[string]*StatementCache
}

// NewManager creates a new statement cache manager
func NewManager() *Manager {
	return &Manager{
		caches: make(map[string]*StatementCache),
	}
}

// GetCache gets or creates a cache for a database
func (m *Manager) GetCache(ctx context.Context, name string, db *sql.DB, cfg *Config) *StatementCache {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cache, exists := m.caches[name]; exists {
		return cache
	}

	cache := NewStatementCache(db, cfg, nil)
	m.caches[name] = cache
	return cache
}

// GetCacheWithBackend gets or creates a cache with a specific backend
func (m *Manager) GetCacheWithBackend(ctx context.Context, name string, db *sql.DB, cfg *Config, backend cache.Cache) *StatementCache {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cache, exists := m.caches[name]; exists {
		return cache
	}

	cache := NewStatementCache(db, cfg, backend)
	m.caches[name] = cache
	return cache
}

// CloseAll closes all caches
func (m *Manager) CloseAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for name, cache := range m.caches {
		if err := cache.Close(); err != nil {
			lastErr = fmt.Errorf("error closing cache %s: %w", name, err)
		}
		delete(m.caches, name)
	}

	return lastErr
}

// CleanAllExpired removes expired statements from all caches
func (m *Manager) CleanAllExpired(ctx context.Context) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, cache := range m.caches {
		cache.CleanExpired(ctx)
	}
}

// Simple in-memory cache implementation
type cacheItem struct {
	value    interface{}
	expiresAt time.Time
}

type simpleCache struct {
	mu    sync.RWMutex
	items map[string]*cacheItem
}

func (c *simpleCache) Get(ctx context.Context, key string, dest interface{}) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	if !exists {
		return cache.ErrCacheMiss
	}

	if time.Now().After(item.expiresAt) {
		return cache.ErrCacheMiss
	}

	// This is a simplified implementation
	// In real usage, you'd need to properly copy the value to dest
	if ptrDest, ok := dest.(*interface{}); ok {
		*ptrDest = item.value
	}

	return nil
}

func (c *simpleCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := time.Now().Add(ttl)
	if ttl == 0 {
		expiresAt = time.Time{} // Never expires
	}

	c.items[key] = &cacheItem{
		value:    value,
		expiresAt: expiresAt,
	}

	return nil
}

func (c *simpleCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
	return nil
}

func (c *simpleCache) Invalidate(ctx context.Context, pattern string) error {
	// Simple implementation - delete all matching keys
	c.mu.Lock()
	defer c.mu.Unlock()

	// For simplicity, just clear all if pattern is "*"
	if pattern == "*" {
		c.items = make(map[string]*cacheItem)
		return nil
	}

	// TODO: Implement pattern matching
	return nil
}

func (c *simpleCache) Clear(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*cacheItem)
	return nil
}

func (c *simpleCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*cacheItem)
	return nil
}

func (c *simpleCache) Name() string {
	return "simple"
}
