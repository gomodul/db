package pool

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/gomodul/db/metrics"
)

// Stats represents connection pool statistics
type Stats struct {
	// Active connections
	OpenConnections int
	InUse           int
	Idle            int

	// Wait statistics
	WaitCount         int64
	WaitDuration     time.Duration
	MaxIdleClosed    int64
	MaxLifetimeClosed int64

	// Pool configuration
	MaxOpenConnections int
	MaxIdleConnections int

	// Timestamp
	Timestamp time.Time
}

// Monitor provides connection pool monitoring capabilities
type Monitor struct {
	db    *sql.DB
	name  string
	mu    sync.RWMutex

	// Statistics collection
	collectInterval time.Duration
	statsHistory    []*Stats
	maxHistory      int

	// Alerting
	alertThresholds AlertThresholds

	// Metrics collector
	metrics metrics.Collector
}

// AlertThresholds defines thresholds for alerts
type AlertThresholds struct {
	MaxOpenConnectionsUsage float64 // Percentage (0.0 - 1.0)
	MaxWaitDuration         time.Duration
	MaxIdleClosedRate       float64 // Closes per second
}

// DefaultAlertThresholds returns default alert thresholds
func DefaultAlertThresholds() AlertThresholds {
	return AlertThresholds{
		MaxOpenConnectionsUsage: 0.9, // 90%
		MaxWaitDuration:         5 * time.Second,
		MaxIdleClosedRate:       10.0, // 10 per second
	}
}

// Config holds configuration for the pool monitor
type Config struct {
	Name            string
	CollectInterval time.Duration
	MaxHistory      int
	AlertThresholds AlertThresholds
	Metrics         metrics.Collector
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Name:            "default",
		CollectInterval: 30 * time.Second,
		MaxHistory:      100,
		AlertThresholds: DefaultAlertThresholds(),
		Metrics:         metrics.NewDefaultCollector(),
	}
}

// NewMonitor creates a new connection pool monitor
func NewMonitor(db *sql.DB, cfg *Config) *Monitor {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return &Monitor{
		db:               db,
		name:             cfg.Name,
		collectInterval:  cfg.CollectInterval,
		statsHistory:     make([]*Stats, 0, cfg.MaxHistory),
		maxHistory:       cfg.MaxHistory,
		alertThresholds:  cfg.AlertThresholds,
		metrics:          cfg.Metrics,
	}
}

// GetStats returns current pool statistics
func (m *Monitor) GetStats(ctx context.Context) (*Stats, error) {
	if m.db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	dbStats := m.db.Stats()
	stats := &Stats{
		OpenConnections:    dbStats.OpenConnections,
		InUse:              dbStats.InUse,
		Idle:               dbStats.Idle,
		WaitCount:          dbStats.WaitCount,
		WaitDuration:       dbStats.WaitDuration,
		MaxIdleClosed:      dbStats.MaxIdleClosed,
		MaxLifetimeClosed:  dbStats.MaxLifetimeClosed,
		MaxOpenConnections: dbStats.MaxOpenConnections,
		Timestamp:          time.Now(),
	}

	// Try to get max idle connections
	if dbStats.MaxOpenConnections > 0 {
		stats.MaxIdleConnections = dbStats.MaxOpenConnections / 2
	}

	return stats, nil
}

// CollectStats collects statistics and stores them in history
func (m *Monitor) CollectStats(ctx context.Context) (*Stats, error) {
	stats, err := m.GetStats(ctx)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Add to history
	m.statsHistory = append(m.statsHistory, stats)
	if len(m.statsHistory) > m.maxHistory {
		m.statsHistory = m.statsHistory[1:]
	}

	// Update metrics
	if m.metrics != nil {
		m.metrics.RecordGauge("pool.open_connections", float64(stats.OpenConnections))
		m.metrics.RecordGauge("pool.in_use", float64(stats.InUse))
		m.metrics.RecordGauge("pool.idle", float64(stats.Idle))
		m.metrics.RecordCounter("pool.wait_count", float64(stats.WaitCount))
		m.metrics.RecordTiming("pool.wait_duration", stats.WaitDuration)
		m.metrics.RecordCounter("pool.max_idle_closed", float64(stats.MaxIdleClosed))
		m.metrics.RecordCounter("pool.max_lifetime_closed", float64(stats.MaxLifetimeClosed))
	}

	return stats, nil
}

// GetHistory returns historical statistics
func (m *Monitor) GetHistory() []*Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	history := make([]*Stats, len(m.statsHistory))
	copy(history, m.statsHistory)
	return history
}

// CheckHealth checks the health of the connection pool
func (m *Monitor) CheckHealth(ctx context.Context) error {
	if m.db == nil {
		return fmt.Errorf("database connection is nil")
	}

	// Ping the database
	if err := m.db.PingContext(ctx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	return nil
}

// GetHealthStatus returns detailed health status
func (m *Monitor) GetHealthStatus(ctx context.Context) (*HealthStatus, error) {
	stats, err := m.GetStats(ctx)
	if err != nil {
		return nil, err
	}

	status := &HealthStatus{
		Healthy: true,
		Stats:   stats,
	}

	// Check if pool is healthy
	if stats.MaxOpenConnections > 0 {
		usage := float64(stats.InUse) / float64(stats.MaxOpenConnections)
		if usage > m.alertThresholds.MaxOpenConnectionsUsage {
			status.Healthy = false
			status.Warnings = append(status.Warnings,
				fmt.Sprintf("High connection usage: %.2f%% (%d/%d)",
					usage*100, stats.InUse, stats.MaxOpenConnections))
		}
	}

	// Check wait duration
	if stats.WaitDuration > 0 {
		avgWait := stats.WaitDuration / time.Duration(stats.WaitCount)
		if avgWait > m.alertThresholds.MaxWaitDuration {
			status.Healthy = false
			status.Warnings = append(status.Warnings,
				fmt.Sprintf("High average wait duration: %v", avgWait))
		}
	}

	// Check connection close rates
	history := m.GetHistory()
	if len(history) >= 2 {
		last := history[len(history)-1]
		prev := history[len(history)-2]
		duration := last.Timestamp.Sub(prev.Timestamp).Seconds()

		if duration > 0 {
			idleCloseRate := float64(last.MaxIdleClosed-prev.MaxIdleClosed) / duration
			lifetimeCloseRate := float64(last.MaxLifetimeClosed-prev.MaxLifetimeClosed) / duration

			if idleCloseRate > m.alertThresholds.MaxIdleClosedRate {
				status.Healthy = false
				status.Warnings = append(status.Warnings,
					fmt.Sprintf("High idle connection close rate: %.2f/sec", idleCloseRate))
			}

			if lifetimeCloseRate > m.alertThresholds.MaxIdleClosedRate {
				status.Healthy = false
				status.Warnings = append(status.Warnings,
					fmt.Sprintf("High lifetime connection close rate: %.2f/sec", lifetimeCloseRate))
			}
		}
	}

	return status, nil
}

// HealthStatus represents the health status of the connection pool
type HealthStatus struct {
	Healthy   bool
	Stats     *Stats
	Warnings  []string
	Timestamp time.Time
}

// StartCollection starts automatic statistics collection
func (m *Monitor) StartCollection(ctx context.Context) {
	ticker := time.NewTicker(m.collectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = m.CollectStats(ctx)
		}
	}
}

// GetPoolInfo returns information about the connection pool configuration
func (m *Monitor) GetPoolInfo() *PoolInfo {
	if m.db == nil {
		return nil
	}

	dbStats := m.db.Stats()
	return &PoolInfo{
		Name:               m.name,
		MaxOpenConnections: dbStats.MaxOpenConnections,
		MaxIdleConnections: m.maxIdleFromStats(dbStats),
		CurrentOpen:        dbStats.OpenConnections,
		InUse:              dbStats.InUse,
		Idle:               dbStats.Idle,
		WaitCount:          dbStats.WaitCount,
		TotalWaitDuration:  dbStats.WaitDuration,
	}
}

// PoolInfo represents connection pool information
type PoolInfo struct {
	Name               string
	MaxOpenConnections int
	MaxIdleConnections int
	CurrentOpen        int
	InUse              int
	Idle               int
	WaitCount          int64
	TotalWaitDuration  time.Duration
}

// maxIdleFromStats attempts to determine max idle connections
func (m *Monitor) maxIdleFromStats(stats sql.DBStats) int {
	// This is a heuristic - the actual max idle may vary
	if stats.MaxOpenConnections > 0 {
		return stats.MaxOpenConnections / 2
	}
	return 2 // Default
}

// Manager manages multiple pool monitors
type Manager struct {
	mu    sync.RWMutex
	pools map[string]*Monitor
}

// NewManager creates a new pool monitor manager
func NewManager() *Manager {
	return &Manager{
		pools: make(map[string]*Monitor),
	}
}

// Register registers a new pool monitor
func (mgr *Manager) Register(db *sql.DB, cfg *Config) *Monitor {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if cfg == nil {
		cfg = DefaultConfig()
	}

	monitor := NewMonitor(db, cfg)
	mgr.pools[cfg.Name] = monitor
	return monitor
}

// Get gets a monitor by name
func (mgr *Manager) Get(name string) (*Monitor, bool) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	monitor, exists := mgr.pools[name]
	return monitor, exists
}

// List returns all registered monitor names
func (mgr *Manager) List() []string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	names := make([]string, 0, len(mgr.pools))
	for name := range mgr.pools {
		names = append(names, name)
	}
	return names
}

// GetAllStats returns statistics for all monitored pools
func (mgr *Manager) GetAllStats(ctx context.Context) map[string]*Stats {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	result := make(map[string]*Stats)
	for name, monitor := range mgr.pools {
		if stats, err := monitor.GetStats(ctx); err == nil {
			result[name] = stats
		}
	}
	return result
}

// CheckAllHealth checks health of all monitored pools
func (mgr *Manager) CheckAllHealth(ctx context.Context) map[string]*HealthStatus {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	result := make(map[string]*HealthStatus)
	for name, monitor := range mgr.pools {
		if status, err := monitor.GetHealthStatus(ctx); err == nil {
			result[name] = status
		}
	}
	return result
}

// Close closes all monitors
func (mgr *Manager) Close() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.pools = make(map[string]*Monitor)
	return nil
}
