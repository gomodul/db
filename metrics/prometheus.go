package metrics

import (
	"context"
	"sync"
	"time"
)

// Metrics collects database operation metrics
type Metrics struct {
	mu sync.RWMutex

	// Query metrics
	queriesTotal     map[string]int64           // operation -> count
	queryDuration    map[string]time.Duration   // operation -> total duration
	queryErrors      map[string]int64           // operation -> error count
	queryLatencies   map[string][]time.Duration // operation -> individual latencies

	// Connection metrics
	activeConnections int
	totalConnections  int64
	connectionErrors  int64

	// Cache metrics
	cacheHits    int64
	cacheMisses  int64
	cacheErrors  int64

	// Transaction metrics
	transactionsTotal   int64
	transactionsCommitted int64
	transactionsRolledBack int64
	transactionDuration  time.Duration

	// Configuration
	maxLatencySamples int
	enabled           bool
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		queriesTotal:     make(map[string]int64),
		queryDuration:    make(map[string]time.Duration),
		queryErrors:      make(map[string]int64),
		queryLatencies:   make(map[string][]time.Duration),
		maxLatencySamples: 100, // Keep last 100 latencies per operation
		enabled:           true,
	}
}

// RecordQuery records a query execution
func (m *Metrics) RecordQuery(operation, driver string, duration time.Duration, err error) {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := operation + ":" + driver

	m.queriesTotal[key]++
	m.queryDuration[key] += duration

	// Store latency samples
	if len(m.queryLatencies[key]) >= m.maxLatencySamples {
		// Remove oldest sample
		m.queryLatencies[key] = m.queryLatencies[key][1:]
	}
	m.queryLatencies[key] = append(m.queryLatencies[key], duration)

	if err != nil {
		m.queryErrors[key]++
	}
}

// RecordConnection records a new connection
func (m *Metrics) RecordConnection(active int) {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeConnections = active
	m.totalConnections++
}

// RecordConnectionError records a connection error
func (m *Metrics) RecordConnectionError() {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.connectionErrors++
}

// RecordCacheHit records a cache hit
func (m *Metrics) RecordCacheHit() {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.cacheHits++
}

// RecordCacheMiss records a cache miss
func (m *Metrics) RecordCacheMiss() {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.cacheMisses++
}

// RecordCacheError records a cache error
func (m *Metrics) RecordCacheError() {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.cacheErrors++
}

// RecordTransactionStart records a transaction start
func (m *Metrics) RecordTransactionStart() {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactionsTotal++
}

// RecordTransactionCommit records a transaction commit
func (m *Metrics) RecordTransactionCommit(duration time.Duration) {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactionsCommitted++
	m.transactionDuration += duration
}

// RecordTransactionRollback records a transaction rollback
func (m *Metrics) RecordTransactionRollback() {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactionsRolledBack++
}

// ============ Query Metrics ============

// GetQueryCount returns the total query count for an operation
func (m *Metrics) GetQueryCount(operation, driver string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := operation + ":" + driver
	return m.queriesTotal[key]
}

// GetQueryDuration returns the total query duration for an operation
func (m *Metrics) GetQueryDuration(operation, driver string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := operation + ":" + driver
	return m.queryDuration[key]
}

// GetQueryErrors returns the total query errors for an operation
func (m *Metrics) GetQueryErrors(operation, driver string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := operation + ":" + driver
	return m.queryErrors[key]
}

// GetQueryLatencies returns the query latencies for an operation
func (m *Metrics) GetQueryLatencies(operation, driver string) []time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := operation + ":" + driver
	latencies := m.queryLatencies[key]
	result := make([]time.Duration, len(latencies))
	copy(result, latencies)
	return result
}

// GetAverageQueryLatency returns the average query latency for an operation
func (m *Metrics) GetAverageQueryLatency(operation, driver string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := operation + ":" + driver
	count := m.queriesTotal[key]
	if count == 0 {
		return 0
	}
	return m.queryDuration[key] / time.Duration(count)
}

// ============ Connection Metrics ============

// GetActiveConnections returns the current number of active connections
func (m *Metrics) GetActiveConnections() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.activeConnections
}

// GetTotalConnections returns the total number of connections
func (m *Metrics) GetTotalConnections() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.totalConnections
}

// GetConnectionErrors returns the total number of connection errors
func (m *Metrics) GetConnectionErrors() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.connectionErrors
}

// ============ Cache Metrics ============

// GetCacheHits returns the total cache hits
func (m *Metrics) GetCacheHits() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.cacheHits
}

// GetCacheMisses returns the total cache misses
func (m *Metrics) GetCacheMisses() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.cacheMisses
}

// GetCacheErrors returns the total cache errors
func (m *Metrics) GetCacheErrors() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.cacheErrors
}

// GetCacheHitRate returns the cache hit rate (0.0 to 1.0)
func (m *Metrics) GetCacheHitRate() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := m.cacheHits + m.cacheMisses
	if total == 0 {
		return 0
	}
	return float64(m.cacheHits) / float64(total)
}

// ============ Transaction Metrics ============

// GetTransactionCount returns the total transaction count
func (m *Metrics) GetTransactionCount() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transactionsTotal
}

// GetTransactionCommitted returns the total committed transactions
func (m *Metrics) GetTransactionCommitted() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transactionsCommitted
}

// GetTransactionRolledBack returns the total rolled back transactions
func (m *Metrics) GetTransactionRolledBack() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.transactionsRolledBack
}

// GetAverageTransactionDuration returns the average transaction duration
func (m *Metrics) GetAverageTransactionDuration() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := m.transactionsCommitted
	if count == 0 {
		return 0
	}
	return m.transactionDuration / time.Duration(count)
}

// ============ Utility Methods ============

// Reset resets all metrics
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.queriesTotal = make(map[string]int64)
	m.queryDuration = make(map[string]time.Duration)
	m.queryErrors = make(map[string]int64)
	m.queryLatencies = make(map[string][]time.Duration)
	m.totalConnections = 0
	m.connectionErrors = 0
	m.cacheHits = 0
	m.cacheMisses = 0
	m.cacheErrors = 0
	m.transactionsTotal = 0
	m.transactionsCommitted = 0
	m.transactionsRolledBack = 0
	m.transactionDuration = 0
}

// Enable enables metrics collection
func (m *Metrics) Enable() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.enabled = true
}

// Disable disables metrics collection
func (m *Metrics) Disable() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.enabled = false
}

// IsEnabled returns true if metrics collection is enabled
func (m *Metrics) IsEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.enabled
}

// Snapshot returns a snapshot of all metrics
func (m *Metrics) Snapshot() *Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := &Snapshot{
		Queries:              make(map[string]*QueryMetrics),
		ActiveConnections:    m.activeConnections,
		TotalConnections:     m.totalConnections,
		ConnectionErrors:     m.connectionErrors,
		CacheHits:            m.cacheHits,
		CacheMisses:          m.cacheMisses,
		CacheErrors:          m.cacheErrors,
		TransactionCount:     m.transactionsTotal,
		TransactionCommitted: m.transactionsCommitted,
		TransactionRollback:  m.transactionsRolledBack,
	}

	for key := range m.queriesTotal {
		snapshot.Queries[key] = &QueryMetrics{
			Total:       m.queriesTotal[key],
			Duration:    m.queryDuration[key],
			Errors:      m.queryErrors[key],
			AverageLatency: m.queryDuration[key] / time.Duration(m.queriesTotal[key]),
		}
	}

	return snapshot
}

// Snapshot contains a snapshot of all metrics
type Snapshot struct {
	Queries              map[string]*QueryMetrics
	ActiveConnections    int
	TotalConnections     int64
	ConnectionErrors     int64
	CacheHits            int64
	CacheMisses          int64
	CacheErrors          int64
	TransactionCount     int64
	TransactionCommitted int64
	TransactionRollback  int64
}

// QueryMetrics contains metrics for a specific query operation
type QueryMetrics struct {
	Total           int64
	Duration        time.Duration
	Errors          int64
	AverageLatency  time.Duration
}

// Middleware wraps database operations with metrics collection
type Middleware struct {
	metrics    *Metrics
	driver     string
}

// NewMiddleware creates a new metrics middleware
func NewMiddleware(metrics *Metrics, driver string) *Middleware {
	return &Middleware{
		metrics: metrics,
		driver:  driver,
	}
}

// Record records a query execution
func (m *Middleware) Record(ctx context.Context, operation string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)

	m.metrics.RecordQuery(operation, m.driver, duration, err)

	return err
}

// RecordWithResult records a query execution and returns its result
func (m *Middleware) RecordWithResult(ctx context.Context, operation string, fn func() (interface{}, error)) (interface{}, error) {
	start := time.Now()
	result, err := fn()
	duration := time.Since(start)

	m.metrics.RecordQuery(operation, m.driver, duration, err)

	return result, err
}
