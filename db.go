package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gomodul/db/builder"
	"github.com/gomodul/db/cache"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/internal/security"
	"github.com/gomodul/db/logger"
	"github.com/gomodul/db/metrics"
	"github.com/gomodul/db/migrate"
	"github.com/gomodul/db/pool"
	"github.com/gomodul/db/query"
)

// DB is the main database handle. It wraps a driver connection and provides
// a fluent API for building and executing queries.
//
// Example:
//	db, err := db.Open(db.Config{DSN: "postgres://user:pass@localhost:5432/mydb"})
//	defer db.Close()
//
//	// Use the universal driver
//	users, err := db.Model(&User{}).Where("status = ?", "active").Find(&users)
type DB struct {
	*Config

	// ============================================================
	// LEGACY FIELDS - Deprecated, will be removed in v2.0.0
	// ============================================================
	//
	// DEPRECATED: Use Driver instead. This field is kept for backward compatibility
	// and will be removed in future versions. The new Driver interface provides
	// universal support across all database types (SQL, NoSQL, APIs, etc.)
	//
	// Migration guide:
	//   OLD: db.SQL.Exec(...)
	//   NEW: db.Driver.Execute(ctx, query)
	SQL *sql.DB // SQL database connection for SQL databases (deprecated)

	// DEPRECATED: Use Driver instead. The Client interface was an intermediate
	// abstraction that has been superseded by the universal Driver interface.
	//
	// Migration guide:
	//   OLD: db.Client.Query(...)
	//   NEW: db.Driver.Execute(ctx, query)
	Client Client // Generic client interface for NoSQL/API databases (deprecated)

	// DEPRECATED: Use Driver instead. The Dialector interface has been replaced
	// by the more comprehensive Driver interface that supports all database types.
	//
	// Migration guide:
	//   OLD: db.Dialector.Name()
	//   NEW: db.Driver.Type()
	Dialector Dialector // The old dialector interface (deprecated)

	// ============================================================
	// NEW FIELDS - Universal Driver Support
	// ============================================================
	//
	// Driver is the universal driver that supports all database types:
	// - SQL databases: PostgreSQL, MySQL, SQLite, etc.
	// - NoSQL databases: MongoDB, Redis, Elasticsearch, etc.
	// - APIs: REST, GraphQL, gRPC, Kafka, etc.
	//
	// The Driver interface provides a single, consistent API for all database
	// operations regardless of the underlying backend.
	Driver dialect.Driver // The new universal driver (use this)

	// Caps contains the driver's capabilities and features
	// Use this to check what operations are supported by the current driver.
	Caps *dialect.Capabilities

	// ============================================================
	// CROSS-CUTTING CONCERNS
	// ============================================================

	// Logger handles query logging and metrics
	Logger logger.Logger

	// QueryCache caches query results for improved performance
	QueryCache *cache.QueryCache

	// PoolMonitor monitors connection pool statistics and health
	PoolMonitor *pool.Monitor

	// Metrics collector for custom metrics
	Metrics metrics.Collector

	// ============================================================
	// QUERY STATE
	// ============================================================

	Error        error    // Last error encountered
	RowsAffected int64    // Number of rows affected by last operation
	Statement    *Statement // Last statement information
	clone        int      // Internal: clone counter for session management
}

// IsLegacyMode returns true if the DB is using legacy fields (SQL/Client/Dialector)
// instead of the new Driver interface.
func (db *DB) IsLegacyMode() bool {
	return db.SQL != nil || db.Client != nil || db.Dialector != nil
}

// GetDriver returns the active driver, with fallback to legacy fields for backward compatibility.
// This method helps migrate from legacy to new Driver interface.
func (db *DB) GetDriver() dialect.Driver {
	// Prefer new Driver interface
	if db.Driver != nil {
		return db.Driver
	}

	// Fallback to legacy fields for backward compatibility
	if db.SQL != nil {
		// TODO: Create a wrapper that converts sql.DB to dialect.Driver
		return nil
	}
	if db.Client != nil {
		// TODO: Create a wrapper that converts Client to dialect.Driver
		return nil
	}

	return nil
}

// UseDriver switches to using the new Driver interface exclusively.
// This is the recommended method for migrating from legacy code.
func (db *DB) UseDriver(driver dialect.Driver) {
	db.Driver = driver
	db.Caps = driver.Capabilities()

	// Clear legacy fields to force using new Driver
	db.SQL = nil
	db.Client = nil
	db.Dialector = nil
}

// Open opens a database connection using the registered driver for the given DSN.
//
// Drivers must be imported for their side effects to register themselves:
//
//	import _ "github.com/gomodul/db/driver/postgres"
func Open(cfg Config) (*DB, error) {
	// Auto-detect driver from DSN
	drivers, err := GetDriverFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	// Convert driver config
	driverCfg := &dialect.Config{
		DSN:            cfg.DSN,
		MaxOpenConns:   cfg.MaxOpenConns,
		MaxIdleConns:   cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		RetryMaxRetries: cfg.RetryMaxRetries,
		RetryBaseDelay:  cfg.RetryBaseDelay,
		RetryMaxDelay:   cfg.RetryMaxDelay,
	}

	// Initialize the driver
	if err := drivers.Initialize(driverCfg); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	db := &DB{
		Config: &cfg,
	}

	// Initialize logger
	if cfg.Logger != nil {
		db.Logger = cfg.Logger
	} else if !cfg.DisableLogger && cfg.LoggerConfig != nil {
		db.Logger = logger.NewSQLQueryLogger(cfg.LoggerConfig)
	} else if !cfg.DisableLogger {
		// Use default logger config
		db.Logger = logger.NewSQLQueryLogger(logger.DefaultConfig())
	} else {
		db.Logger = logger.NewNullLogger()
	}

	// Initialize cache
	if !cfg.DisableCache && cfg.Cache != nil {
		ttl := cfg.CacheTTL
		if ttl == 0 {
			ttl = 5 * time.Minute
		}
		db.QueryCache = cache.NewQueryCache(cfg.Cache, ttl)
	}

	// Wrap driver with retry if configured
	if cfg.RetryMaxRetries > 0 {
		baseDelay := cfg.RetryBaseDelay
		if baseDelay == 0 {
			baseDelay = 100 * time.Millisecond
		}
		maxDelay := cfg.RetryMaxDelay
		if maxDelay == 0 {
			maxDelay = time.Second
		}
		db.Driver = dialect.NewRetryableDriver(drivers, cfg.RetryMaxRetries, baseDelay, maxDelay)
	} else {
		db.Driver = drivers
	}

	// Get capabilities
	db.Caps = db.Driver.Capabilities()

	return db, nil
}

// OpenWithDriver opens a database connection with a specific driver
func OpenWithDriver(drvr dialect.Driver, cfg Config) (*DB, error) {
	driverCfg := &dialect.Config{
		DSN:            cfg.DSN,
		MaxOpenConns:   cfg.MaxOpenConns,
		MaxIdleConns:   cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
		RetryMaxRetries: cfg.RetryMaxRetries,
		RetryBaseDelay:  cfg.RetryBaseDelay,
		RetryMaxDelay:   cfg.RetryMaxDelay,
	}

	// Initialize the driver
	if err := drvr.Initialize(driverCfg); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	db := &DB{
		Config: &cfg,
	}

	// Wrap driver with retry if configured
	if cfg.RetryMaxRetries > 0 {
		baseDelay := cfg.RetryBaseDelay
		if baseDelay == 0 {
			baseDelay = 100 * time.Millisecond
		}
		maxDelay := cfg.RetryMaxDelay
		if maxDelay == 0 {
			maxDelay = time.Second
		}
		db.Driver = dialect.NewRetryableDriver(drvr, cfg.RetryMaxRetries, baseDelay, maxDelay)
	} else {
		db.Driver = drvr
	}

	db.Caps = db.Driver.Capabilities()

	return db, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	if db.Driver != nil {
		return db.Driver.Close()
	}
	if db.SQL != nil {
		return db.SQL.Close()
	}
	return nil
}

// Session creates a new session for the database
func (db *DB) Session() *DB {
	return &DB{
		Config:  db.Config,
		Driver:  db.Driver,
		Caps:    db.Caps,
		SQL:     db.SQL,
		Client:  db.Client,
		Dialector: db.Dialector,
		clone:   1,
	}
}

// DB returns the underlying *sql.DB for SQL databases (legacy)
func (db *DB) DB() (*sql.DB, error) {
	if db.SQL != nil {
		return db.SQL, nil
	}
	return nil, ErrNotSupported
}

// DriverType returns the type of the current driver
func (db *DB) DriverType() dialect.DriverType {
	if db.Driver != nil {
		return db.Driver.Type()
	}
	return ""
}

// Capabilities returns the driver's capabilities
func (db *DB) Capabilities() *dialect.Capabilities {
	return db.Caps
}

// HasCapability checks if the driver has a specific capability
func (db *DB) HasCapability(feature string) bool {
	if db.Caps == nil {
		return false
	}
	return db.Caps.HasFeature(feature)
}

// Ping checks if the database is reachable
func (db *DB) Ping(ctx context.Context) error {
	if db.Driver == nil {
		return ErrNotSupported
	}

	// Check if driver implements Conn interface (has Ping method)
	if conn, ok := db.Driver.(dialect.Conn); ok {
		return conn.Ping(ctx)
	}
	return ErrNotSupported
}

// Health checks the health of the database connection
func (db *DB) Health() (*dialect.HealthStatus, error) {
	if db.Driver == nil {
		return dialect.NewUnhealthyStatus("no driver configured"), nil
	}

	// Use the helper function from dialect package
	return dialect.Health(db.Driver)
}

// Transaction executes a function within a transaction (if supported)
func (db *DB) Transaction(fn func(tx *DB) error) error {
	if db.Driver == nil || !db.Caps.Transaction.Supported {
		return ErrNotSupported
	}

	// Use the helper function from dialect package
	tx, err := dialect.BeginTx(context.Background(), db.Driver)
	if err != nil {
		return err
	}

	txDB := &DB{
		Config: db.Config,
		Driver: db.Driver,
		Caps:   db.Caps,
	}

	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()

	if err := fn(txDB); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Migrate runs auto-migration (if supported)
func (db *DB) Migrate(models ...interface{}) error {
	if db.Driver == nil {
		return ErrNotSupported
	}

	// Use the helper function from dialect package
	migrator := dialect.GetMigrator(db.Driver)
	return migrator.AutoMigrate(models...)
}

// Execute executes a universal query
func (db *DB) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	if db.Driver == nil {
		return nil, ErrNotSupported
	}
	return db.Driver.Execute(ctx, q)
}

// Legacy methods (for backward compatibility)

// Exec executes a query without returning any rows (legacy SQL method)
//
// SECURITY WARNING: Always use parameterized queries with placeholders.
//   ✅ GOOD: db.Exec("UPDATE users SET name = ? WHERE id = ?", "John", 1)
//   ❌ BAD:  db.Exec("UPDATE users SET name = '" + name + "' WHERE id = " + id)
func (db *DB) Exec(sql string, values ...interface{}) *DB {
	// Validate raw query for security
	if warnings, err := security.ValidateRawQuery(sql, nil); err != nil {
		db.Error = fmt.Errorf("raw query validation failed: %w", err)
		return db
	} else if len(warnings) > 0 && db.Logger != nil {
		// Log warnings
		for _, w := range warnings {
			db.Logger.Log(context.Background(), logger.Warn, fmt.Sprintf("Raw query security warning [%s]: %s\nQuery: %s", w.Severity, w.Message, sql))
		}
	}

	if db.SQL != nil {
		result, err := db.SQL.Exec(sql, values...)
		if err != nil {
			db.Error = security.AddSecurityWarningToError(err, sql)
			return db
		}
		db.RowsAffected, _ = result.RowsAffected()
	} else if db.Driver != nil {
		// Try to execute through new driver
		q := &query.Query{
			Raw:    sql,
			RawArgs: values,
			IsRaw:  true,
		}
		result, err := db.Driver.Execute(context.Background(), q)
		if err != nil {
			db.Error = security.AddSecurityWarningToError(err, sql)
			return db
		}
		db.RowsAffected = result.RowsAffected
	}
	return db
}

// Raw creates a raw SQL query (legacy)
//
// SECURITY WARNING: Always use parameterized queries with placeholders.
//   ✅ GOOD: db.Raw("SELECT * FROM users WHERE id = ?", userID)
//   ❌ BAD:  db.Raw("SELECT * FROM users WHERE id = " + userID)
func (db *DB) Raw(sql string, values ...interface{}) *DB {
	// Validate raw query for security
	if warnings, err := security.ValidateRawQuery(sql, nil); err != nil {
		db.Error = fmt.Errorf("raw query validation failed: %w", err)
		return db
	} else if len(warnings) > 0 && db.Logger != nil {
		// Log warnings
		for _, w := range warnings {
			db.Logger.Log(context.Background(), logger.Warn, fmt.Sprintf("Raw query security warning [%s]: %s\nQuery: %s", w.Severity, w.Message, sql))
		}
	}

	if db.SQL != nil {
		rows, err := db.SQL.Query(sql, values...)
		if err != nil {
			db.Error = security.AddSecurityWarningToError(err, sql)
			return db
		}
		defer rows.Close()
	} else if db.Driver != nil {
		q := &query.Query{
			Operation: query.OpFind,
			Raw:       sql,
			RawArgs:    values,
			IsRaw:     true,
		}
		_, err := db.Driver.Execute(context.Background(), q)
		if err != nil {
			db.Error = security.AddSecurityWarningToError(err, sql)
			return db
		}
	}
	return db
}

// Scan scans the result into a destination (legacy placeholder)
func (db *DB) Scan(dest interface{}) *DB {
	// Implementation for scanning results
	// This will be replaced by the new query builder
	return db
}

// Model starts a query on a model using the fluent builder API
func (db *DB) Model(value interface{}) *builder.QueryBuilder {
	b := &builder.DB{
		Dialect: db.Driver,
		Caps:    db.Caps,
	}
	return builder.New(b, value)
}

// Table starts a query on a specific table using the fluent builder API
func (db *DB) Table(name string) *builder.QueryBuilder {
	b := &builder.DB{
		Dialect: db.Driver,
		Caps:    db.Caps,
	}
	return builder.New(b, nil).Table(name)
}

// ============ Connection Pool Monitoring Methods ============

// GetPoolStats returns current connection pool statistics
// Returns nil if the driver doesn't support connection pooling (e.g., NoSQL/API drivers)
func (db *DB) GetPoolStats(ctx context.Context) (*pool.Stats, error) {
	if db.PoolMonitor != nil {
		return db.PoolMonitor.GetStats(ctx)
	}

	// Fallback to SQL driver stats
	if db.SQL != nil {
		stats := db.SQL.Stats()
		return &pool.Stats{
			OpenConnections:    stats.OpenConnections,
			InUse:              stats.InUse,
			Idle:               stats.Idle,
			WaitCount:          stats.WaitCount,
			WaitDuration:       stats.WaitDuration,
			MaxIdleClosed:      stats.MaxIdleClosed,
			MaxLifetimeClosed:  stats.MaxLifetimeClosed,
			MaxOpenConnections: stats.MaxOpenConnections,
			Timestamp:          time.Now(),
		}, nil
	}

	return nil, ErrNotSupported
}

// GetPoolHealth checks the health of the connection pool
// Returns nil if the driver doesn't support connection pooling
func (db *DB) GetPoolHealth(ctx context.Context) (*pool.HealthStatus, error) {
	if db.PoolMonitor != nil {
		return db.PoolMonitor.GetHealthStatus(ctx)
	}

	// Fallback to basic health check
	if db.SQL != nil {
		stats := db.SQL.Stats()
		status := &pool.HealthStatus{
			Healthy: true,
			Timestamp: time.Now(),
		}

		// Basic health check
		if stats.MaxOpenConnections > 0 {
			usage := float64(stats.InUse) / float64(stats.MaxOpenConnections)
			if usage > 0.9 {
				status.Healthy = false
				status.Warnings = append(status.Warnings,
					fmt.Sprintf("High connection usage: %.2f%%", usage*100))
			}
		}

		return status, nil
	}

	return nil, ErrNotSupported
}

// EnablePoolMonitoring enables connection pool monitoring
// This only works for SQL drivers with connection pooling
func (db *DB) EnablePoolMonitoring(cfg *pool.Config) error {
	if db.SQL == nil {
		return ErrNotSupported
	}

	if cfg == nil {
		cfg = pool.DefaultConfig()
		cfg.Name = "default"
	}

	db.PoolMonitor = pool.NewMonitor(db.SQL, cfg)
	db.Metrics = cfg.Metrics

	return nil
}

// DisablePoolMonitoring disables connection pool monitoring
func (db *DB) DisablePoolMonitoring() {
	db.PoolMonitor = nil
}

// GetPoolInfo returns connection pool information
func (db *DB) GetPoolInfo() *pool.PoolInfo {
	if db.PoolMonitor != nil {
		return db.PoolMonitor.GetPoolInfo()
	}

	// Fallback to SQL driver stats
	if db.SQL != nil {
		stats := db.SQL.Stats()
		return &pool.PoolInfo{
			Name:               "default",
			MaxOpenConnections: stats.MaxOpenConnections,
			CurrentOpen:        stats.OpenConnections,
			InUse:              stats.InUse,
			Idle:               stats.Idle,
			WaitCount:          stats.WaitCount,
			TotalWaitDuration:  stats.WaitDuration,
		}
	}

	return nil
}

// CollectPoolMetrics collects and records pool metrics
func (db *DB) CollectPoolMetrics(ctx context.Context) error {
	if db.PoolMonitor != nil {
		_, err := db.PoolMonitor.CollectStats(ctx)
		return err
	}
	return ErrNotSupported
}

// ============ Schema Migration Methods ============

// AutoMigrate automatically creates/updates database schema for the given models
// This is a convenient method that uses the migrate package internally
//
// Example:
//
//	err := database.AutoMigrate(&User{}, &Order{}, &Product{})
func (db *DB) AutoMigrate(models ...interface{}) error {
	ctx := context.Background()

	// Use SQL database directly if available (for legacy mode)
	if db.SQL != nil {
		migrator := migrate.NewMigrator(&sqlDB{db: db.SQL}, db.Driver)
		return migrator.AutoMigrate(ctx, models...)
	}

	// For new driver interface, we need to check if it supports migrations
	if db.Driver != nil {
		// Check if driver implements the migration interface
		if migrator, ok := db.Driver.(interface{ AutoMigrate(...interface{}) error }); ok {
			return migrator.AutoMigrate(models...)
		}
	}

	return ErrNotSupported
}

// Migrator returns a new Migrator instance for advanced schema operations
//
// Example:
//
//	migrator := db.Migrator()
//	err := migrator.AutoMigrate(ctx, &User{})
//	err = migrator.CreateIndex(ctx, &User{}, &migrate.IndexInfo{
//	    Name:    "idx_user_email",
//	    Columns: []string{"email"},
//	    Unique:  true,
//	})
func (db *DB) Migrator() *migrate.Migrator {
	if db.SQL != nil {
		return migrate.NewMigrator(&sqlDB{db: db.SQL}, db.Driver)
	}
	return migrate.NewMigrator(nil, db.Driver)
}

// sqlDB wraps *sql.DB to implement the migrate.DB interface
type sqlDB struct {
	db *sql.DB
}

// Exec implements migrate.DB
func (s *sqlDB) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := s.db.ExecContext(ctx, sql, args...)
	return err
}

// Query implements migrate.DB
func (s *sqlDB) Query(ctx context.Context, sql string, args ...interface{}) (migrate.Result, error) {
	rows, err := s.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &sqlResult{rows: rows}, nil
}

// sqlResult wraps sql.Rows to implement migrate.Result
type sqlResult struct {
	rows *sql.Rows
}

// Columns implements migrate.Result
func (s *sqlResult) Columns() ([]string, error) {
	return s.rows.Columns()
}

// Close implements migrate.Result
func (s *sqlResult) Close() error {
	return s.rows.Close()
}
