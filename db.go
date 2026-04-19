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
	"github.com/gomodul/db/pool"
	"github.com/gomodul/db/query"
)

// DB is the main database handle. It wraps a driver connection and provides
// a fluent API for building and executing queries.
//
// Example:
//
//	db, err := db.Open(db.Config{DSN: "postgres://user:pass@localhost:5432/mydb"})
//	defer db.Close()
//
//	// Use the universal driver
//	users, err := db.Model(&User{}).Where("status = ?", "active").Find(&users)
type DB struct {
	*Config

	Driver dialect.Driver // universal driver
	Caps   *dialect.Capabilities

	Logger      logger.Logger
	QueryCache  *cache.QueryCache
	PoolMonitor *pool.Monitor
	Metrics     metrics.Collector

	Error        error
	RowsAffected int64
	Statement    *Statement
	clone        int
}

// GetDriver returns the active driver.
func (db *DB) GetDriver() dialect.Driver {
	return db.Driver
}

// UseDriver sets the driver and updates capabilities.
func (db *DB) UseDriver(driver dialect.Driver) {
	db.Driver = driver
	db.Caps = driver.Capabilities()
}

// sqlDB returns the underlying *sql.DB when the driver implements dialect.SQLAccessor.
func (db *DB) sqlDB() *sql.DB {
	if a, ok := db.Driver.(dialect.SQLAccessor); ok {
		return a.UnderlyingSQL()
	}
	return nil
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
		DSN:             cfg.DSN,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
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
		DSN:             cfg.DSN,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
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
	return nil
}

// Session creates a new session for the database.
func (db *DB) Session() *DB {
	return &DB{
		Config: db.Config,
		Driver: db.Driver,
		Caps:   db.Caps,
		clone:  1,
	}
}

// DB returns the underlying *sql.DB for SQL drivers that expose it.
func (db *DB) DB() (*sql.DB, error) {
	if s := db.sqlDB(); s != nil {
		return s, nil
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

// TransactionContext executes fn within a transaction using the provided context.
func (db *DB) Transaction(ctx context.Context, fn func(tx *DB) error) error {
	if db.Driver == nil || !db.Caps.Transaction.Supported {
		return ErrNotSupported
	}

	tx, err := dialect.BeginTx(ctx, db.Driver)
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

// Exec executes a query without returning any rows.
//
// SECURITY WARNING: Always use parameterized queries.
//
//	✅ GOOD: db.Exec(ctx, "UPDATE users SET name = ? WHERE id = ?", "John", 1)
//	❌ BAD:  db.Exec(ctx, "UPDATE users SET name = '" + name + "'")
func (db *DB) Exec(ctx context.Context, sql string, values ...interface{}) *DB {
	if warnings, err := security.ValidateRawQuery(sql, nil); err != nil {
		db.Error = fmt.Errorf("raw query validation failed: %w", err)
		return db
	} else if len(warnings) > 0 && db.Logger != nil {
		for _, w := range warnings {
			db.Logger.Log(ctx, logger.Warn, fmt.Sprintf("raw query security warning [%s]: %s — query: %s", w.Severity, w.Message, sql))
		}
	}

	if db.Driver != nil {
		q := &query.Query{
			Raw:     sql,
			RawArgs: values,
			IsRaw:   true,
		}
		result, err := db.Driver.Execute(ctx, q)
		if err != nil {
			db.Error = security.AddSecurityWarningToError(err, sql)
			return db
		}
		db.RowsAffected = result.RowsAffected
	}
	return db
}

// Raw stores a raw SQL query for deferred execution via Scan.
//
// SECURITY WARNING: Always use parameterized queries with placeholders.
//
//	✅ GOOD: db.Raw("SELECT * FROM users WHERE id = ?", userID).Scan(&users)
//	❌ BAD:  db.Raw("SELECT * FROM users WHERE id = " + userID)
func (db *DB) Raw(sql string, values ...interface{}) *DB {
	if warnings, err := security.ValidateRawQuery(sql, nil); err != nil {
		db.Error = fmt.Errorf("raw query validation failed: %w", err)
		return db
	} else if len(warnings) > 0 && db.Logger != nil {
		for _, w := range warnings {
			db.Logger.Log(context.Background(), logger.Warn,
				fmt.Sprintf("raw query security warning [%s]: %s — query: %s", w.Severity, w.Message, sql))
		}
	}

	clone := db.Session()
	stmt := &Statement{}
	stmt.SQL.WriteString(sql)
	stmt.Vars = values
	clone.Statement = stmt
	return clone
}

// Scan executes the raw query stored by Raw and scans results into dest.
// For fluent query building use Model().Where(...).Find(&dest) instead.
func (db *DB) Scan(dest interface{}) *DB {
	if db.Statement == nil || db.Statement.SQL.Len() == 0 {
		return db
	}
	rawSQL := db.Statement.SQL.String()
	if db.Driver != nil {
		q := &query.Query{
			Operation: query.OpFind,
			Raw:       rawSQL,
			RawArgs:   db.Statement.Vars,
			IsRaw:     true,
		}
		result, err := db.Driver.Execute(db.Statement.Context, q)
		if err != nil {
			db.Error = security.AddSecurityWarningToError(err, rawSQL)
			return db
		}
		if result != nil {
			db.RowsAffected = result.RowsAffected
		}
	}
	return db
}

// Model starts a query on a model using the fluent builder API
func (db *DB) Model(value interface{}) *builder.QueryBuilder {
	b := &builder.DB{
		Dialect: db.Driver,
		Caps:    db.Caps,
		Logger:  db.Logger,
	}
	return builder.New(b, value)
}

// Table starts a query on a specific table using the fluent builder API
func (db *DB) Table(name string) *builder.QueryBuilder {
	b := &builder.DB{
		Dialect: db.Driver,
		Caps:    db.Caps,
		Logger:  db.Logger,
	}
	return builder.New(b, nil).Table(name)
}

// ============ Connection Pool Monitoring Methods ============

// GetPoolStats returns current connection pool statistics.
// Returns ErrNotSupported for drivers that don't expose a *sql.DB.
func (db *DB) GetPoolStats(ctx context.Context) (*pool.Stats, error) {
	if db.PoolMonitor != nil {
		return db.PoolMonitor.GetStats(ctx)
	}
	if s := db.sqlDB(); s != nil {
		st := s.Stats()
		return &pool.Stats{
			OpenConnections:    st.OpenConnections,
			InUse:              st.InUse,
			Idle:               st.Idle,
			WaitCount:          st.WaitCount,
			WaitDuration:       st.WaitDuration,
			MaxIdleClosed:      st.MaxIdleClosed,
			MaxLifetimeClosed:  st.MaxLifetimeClosed,
			MaxOpenConnections: st.MaxOpenConnections,
			Timestamp:          time.Now(),
		}, nil
	}
	return nil, ErrNotSupported
}

// GetPoolHealth checks the health of the connection pool.
func (db *DB) GetPoolHealth(ctx context.Context) (*pool.HealthStatus, error) {
	if db.PoolMonitor != nil {
		return db.PoolMonitor.GetHealthStatus(ctx)
	}
	if s := db.sqlDB(); s != nil {
		st := s.Stats()
		status := &pool.HealthStatus{Healthy: true, Timestamp: time.Now()}
		if st.MaxOpenConnections > 0 {
			usage := float64(st.InUse) / float64(st.MaxOpenConnections)
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

// EnablePoolMonitoring enables connection pool monitoring for SQL drivers.
func (db *DB) EnablePoolMonitoring(cfg *pool.Config) error {
	s := db.sqlDB()
	if s == nil {
		return ErrNotSupported
	}
	if cfg == nil {
		cfg = pool.DefaultConfig()
		cfg.Name = "default"
	}
	db.PoolMonitor = pool.NewMonitor(s, cfg)
	db.Metrics = cfg.Metrics
	return nil
}

// DisablePoolMonitoring disables connection pool monitoring.
func (db *DB) DisablePoolMonitoring() {
	db.PoolMonitor = nil
}

// GetPoolInfo returns connection pool information.
func (db *DB) GetPoolInfo() *pool.PoolInfo {
	if db.PoolMonitor != nil {
		return db.PoolMonitor.GetPoolInfo()
	}
	if s := db.sqlDB(); s != nil {
		st := s.Stats()
		return &pool.PoolInfo{
			Name:               "default",
			MaxOpenConnections: st.MaxOpenConnections,
			CurrentOpen:        st.OpenConnections,
			InUse:              st.InUse,
			Idle:               st.Idle,
			WaitCount:          st.WaitCount,
			TotalWaitDuration:  st.WaitDuration,
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

// AutoMigrate automatically creates/updates database schema for the given models.
//
// Example:
//
//	err := database.AutoMigrate(&User{}, &Order{}, &Product{})
func (db *DB) AutoMigrate(models ...interface{}) error {
	return db.Migrator().AutoMigrate(models...)
}

// Migrator returns a dialect.Migrator for advanced schema operations.
//
// Example:
//
//	m := db.Migrator()
//	err := m.AutoMigrate(&User{})
//	err = m.CreateIndex("users", "idx_email", []string{"email"}, true)
func (db *DB) Migrator() dialect.Migrator {
	return dialect.GetMigrator(db.Driver)
}
