package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gomodul/db/builder"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
	"time"
)

// DB is the main database handle. It wraps a driver connection and provides
// a fluent API for building and executing queries.
//
//	defer db.Close()
type DB struct {
	*Config

	// Legacy fields (for backward compatibility)
	SQL       *sql.DB   // SQL database connection for SQL databases
	Client    Client    // Generic client interface for NoSQL/API databases
	Dialector Dialector // The old dialector interface (deprecated)

	// New fields (universal driver support)
	Driver dialect.Driver // The new universal driver
	Caps  *dialect.Capabilities

	// Query state
	Error        error
	RowsAffected int64
	Statement    *Statement
	clone        int
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
func (db *DB) Exec(sql string, values ...interface{}) *DB {
	if db.SQL != nil {
		result, err := db.SQL.Exec(sql, values...)
		if err != nil {
			db.Error = err
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
			db.Error = err
			return db
		}
		db.RowsAffected = result.RowsAffected
	}
	return db
}

// Raw creates a raw SQL query (legacy)
func (db *DB) Raw(sql string, values ...interface{}) *DB {
	if db.SQL != nil {
		rows, err := db.SQL.Query(sql, values...)
		if err != nil {
			db.Error = err
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
		_, db.Error = db.Driver.Execute(context.Background(), q)
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
