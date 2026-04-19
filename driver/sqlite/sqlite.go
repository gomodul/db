package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
	"github.com/gomodul/db/translator"
)

// Driver implements the dialect.Driver interface for SQLite
type Driver struct {
	db     *sql.DB
	dsn    string
	config *dialect.Config
	trans  *translator.SQLTranslator
}

// NewDriver creates a new SQLite driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "sqlite"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeSQL
}

// Initialize initializes the database connection
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg
	d.dsn = cfg.DSN

	var err error
	d.db, err = sql.Open("sqlite", d.dsn)
	if err != nil {
		return fmt.Errorf("failed to open sqlite: %w", err)
	}

	// Set connection pool settings (SQLite has limited support)
	if cfg.MaxOpenConns > 0 {
		d.db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		d.db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		d.db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		d.db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}

	// Initialize translator
	d.trans = translator.NewSQLTranslator(&SQLiteDialect{})

	// Verify connection
	return d.db.Ping()
}

// Close closes the database connection
func (d *Driver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// Execute executes a universal query
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	// Handle raw queries
	if q.IsRaw {
		return d.executeRaw(ctx, q)
	}

	// Translate universal query to SQL
	backendQuery, err := d.trans.Translate(q)
	if err != nil {
		return nil, err
	}

	sqlQuery, ok := backendQuery.(*translator.SQLQuery)
	if !ok {
		return nil, fmt.Errorf("unexpected query type: %T", backendQuery)
	}

	// Execute SQL query
	switch q.Operation {
	case query.OpFind:
		return d.executeQuery(ctx, sqlQuery, q)
	case query.OpCreate:
		return d.executeCreate(ctx, sqlQuery, q)
	case query.OpUpdate:
		return d.executeUpdate(ctx, sqlQuery)
	case query.OpDelete:
		return d.executeDelete(ctx, sqlQuery)
	case query.OpCount:
		return d.executeCount(ctx, sqlQuery)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", q.Operation)
	}
}

// executeQuery executes a SELECT query
func (d *Driver) executeQuery(ctx context.Context, sqlQuery *translator.SQLQuery, q *query.Query) (*dialect.Result, error) {
	rows, err := d.db.QueryContext(ctx, sqlQuery.SQL, sqlQuery.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Scan rows into data
	data, err := d.scanRows(rows)
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		Data:        data,
		RowsAffected: int64(len(data)),
	}, nil
}

// executeCreate executes an INSERT query
func (d *Driver) executeCreate(ctx context.Context, sqlQuery *translator.SQLQuery, q *query.Query) (*dialect.Result, error) {
	result, err := d.db.ExecContext(ctx, sqlQuery.SQL, sqlQuery.Args...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()
	lastInsertID, _ := result.LastInsertId()

	return &dialect.Result{
		RowsAffected: rowsAffected,
		LastInsertID: lastInsertID,
	}, nil
}

// executeUpdate executes an UPDATE query
func (d *Driver) executeUpdate(ctx context.Context, sqlQuery *translator.SQLQuery) (*dialect.Result, error) {
	result, err := d.db.ExecContext(ctx, sqlQuery.SQL, sqlQuery.Args...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()

	return &dialect.Result{
		RowsAffected: rowsAffected,
	}, nil
}

// executeDelete executes a DELETE query
func (d *Driver) executeDelete(ctx context.Context, sqlQuery *translator.SQLQuery) (*dialect.Result, error) {
	result, err := d.db.ExecContext(ctx, sqlQuery.SQL, sqlQuery.Args...)
	if err != nil {
		return nil, err
	}

	rowsAffected, _ := result.RowsAffected()

	return &dialect.Result{
		RowsAffected: rowsAffected,
	}, nil
}

// executeCount executes a COUNT query
func (d *Driver) executeCount(ctx context.Context, sqlQuery *translator.SQLQuery) (*dialect.Result, error) {
	var count int64
	err := d.db.QueryRowContext(ctx, sqlQuery.SQL, sqlQuery.Args...).Scan(&count)
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		Count: count,
	}, nil
}

// executeRaw executes a raw SQL query
func (d *Driver) executeRaw(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	switch q.Operation {
	case query.OpFind, query.OpAggregate:
		rows, err := d.db.QueryContext(ctx, q.Raw, q.RawArgs...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		data, err := d.scanRows(rows)
		if err != nil {
			return nil, err
		}

		return &dialect.Result{
			Data:        data,
			RowsAffected: int64(len(data)),
		}, nil

	default:
		result, err := d.db.ExecContext(ctx, q.Raw, q.RawArgs...)
		if err != nil {
			return nil, err
		}

		rowsAffected, _ := result.RowsAffected()

		return &dialect.Result{
			RowsAffected: rowsAffected,
		}, nil
	}
}

// scanRows scans SQL rows into a slice of maps
func (d *Driver) scanRows(rows *sql.Rows) ([]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []interface{}

	for rows.Next() {
		// Create a slice of interface{} to hold each column value
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Convert to map[string]interface{}
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Capabilities returns the driver's capabilities
func (d *Driver) Capabilities() *dialect.Capabilities {
	return &dialect.Capabilities{
		Query: dialect.QueryCapabilities{
			Create:         true,
			Read:           true,
			Update:         true,
			Delete:         true,
			BatchCreate:    true,
			BatchUpdate:    false, // Limited support in SQLite
			BatchDelete:    true,
			Filters:        allFilterOperators(),
			Sort:           true,
			MultiFieldSort: true,
			OffsetPagination: true,
			GroupBy:         true,
			Aggregations:    allAggregationOperators(),
			Joins:           true,
			NestedJoins:     true,
			Subqueries:      true,
			Unions:          true,
			Hints:           false,
			Locking:         false,
		},
		Transaction: dialect.TransactionCapabilities{
			Supported:       true,
			Nested:          false,
			Savepoints:      true,
			IsolationLevels: sqliteIsolationLevels(),
		},
		Schema: dialect.SchemaCapabilities{
			AutoMigrate:      true,
			CreateTables:     true,
			AlterTables:      true,
			DropTables:       true,
			CreateIndexes:    true,
			DropIndexes:      true,
			Constraints:      true,
			ForeignKeys:      true,
			CheckConstraints: true,
		},
		Indexing: dialect.IndexCapabilities{
			Unique:    true,
			Composite: true,
			Partial:   false,
			FullText:  true, // FTS5 extension
			BTree:     true,
			Hash:      false,
		},
	}
}

// Ping checks if the database is reachable
func (d *Driver) Ping(ctx context.Context) error {
	return d.db.PingContext(ctx)
}

// Health returns the health status
func (d *Driver) Health() (*dialect.HealthStatus, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.Ping(ctx); err != nil {
		return dialect.NewUnhealthyStatus(err.Error()), nil
	}

	// Get connection pool stats
	stats := d.db.Stats()
	return dialect.NewHealthyStatus(time.Since(start)).
		WithDetail("open_connections", stats.OpenConnections).
		WithDetail("in_use", stats.InUse).
		WithDetail("idle", stats.Idle), nil
}

// BeginTx starts a new transaction
func (d *Driver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &SQLiteTx{tx: tx, driver: d}, nil
}

// SQLiteTx represents a SQLite transaction
type SQLiteTx struct {
	tx     *sql.Tx
	driver *Driver
}

// Commit commits the transaction
func (t *SQLiteTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction
func (t *SQLiteTx) Rollback() error {
	return t.tx.Rollback()
}

// Query executes a query within the transaction
func (t *SQLiteTx) Query(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	return t.driver.Execute(ctx, q)
}

// Exec executes a command without returning rows
func (t *SQLiteTx) Exec(ctx context.Context, rawSQL string, args ...interface{}) (*dialect.Result, error) {
	q := &query.Query{
		Raw:     rawSQL,
		RawArgs: args,
		IsRaw:   true,
	}
	return t.driver.Execute(ctx, q)
}

// SQLiteDialect implements translator.SQLDialect for SQLite
type SQLiteDialect struct{}

// Name returns the dialect name
func (d *SQLiteDialect) Name() string {
	return "sqlite"
}

// BindVar returns the bind variable format
func (d *SQLiteDialect) BindVar(idx int) string {
	return "?"
}

// QuoteIdentifier quotes an identifier
func (d *SQLiteDialect) QuoteIdentifier(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

// Supports checks if a feature is supported
func (d *SQLiteDialect) Supports(feature translator.SQLFeature) bool {
	switch feature {
	case translator.FeatureCTE,
		translator.FeatureUpsert:
		return true
	default:
		return false
	}
}

// Helper functions for capabilities

func allFilterOperators() []query.FilterOperator {
	return []query.FilterOperator{
		query.OpEqual,
		query.OpNotEqual,
		query.OpGreaterThan,
		query.OpGreaterOrEqual,
		query.OpLessThan,
		query.OpLessOrEqual,
		query.OpIn,
		query.OpNotIn,
		query.OpLike,
		query.OpNotLike,
		query.OpBetween,
		query.OpNull,
		query.OpNotNull,
		query.OpContains,
		query.OpStartsWith,
		query.OpEndsWith,
	}
}

func allAggregationOperators() []query.AggOperator {
	return []query.AggOperator{
		query.AggOpCount,
		query.AggOpSum,
		query.AggOpAvg,
		query.AggOpMin,
		query.AggOpMax,
	}
}

func sqliteIsolationLevels() []dialect.IsolationLevel {
	// SQLite only supports SERIALIZABLE isolation level
	return []dialect.IsolationLevel{
		dialect.LevelSerializable,
	}
}

// UnderlyingSQL returns the underlying *sql.DB for pool monitoring.
func (d *Driver) UnderlyingSQL() *sql.DB {
	return d.db
}

// Migrator returns the dialect.Migrator for schema operations
func (d *Driver) Migrator() dialect.Migrator {
	return &Migrator{driver: d}
}

func init() {
	// Register the SQLite driver
	dialect.Register("sqlite", func() dialect.Driver {
		return NewDriver()
	})
	dialect.Register("sqlite3", func() dialect.Driver {
		return NewDriver()
	})
}
