package db

import (
	"context"

	"github.com/gomodul/db/builder"
)

// DB is the main database handle. It wraps a driver connection and provides
// a fluent API for building and executing queries.
//
// Open a connection using Open:
//
//	db, err := db.Open(db.Config{
//	    Engine: db.EnginePostgreSQL,
//	    DSN:    "postgres://localhost/mydb",
//	})
//	defer db.Close()
type DB struct {
	conn   Conn
	engine Engine
}

// Open opens a database connection using the registered driver for the given engine.
//
// Drivers must be imported for their side effects to register themselves:
//
//	import _ "github.com/gomodul/db/driver/postgres"
func Open(cfg Config) (*DB, error) {
	d, err := getDriver(cfg.Engine)
	if err != nil {
		return nil, err
	}

	conn, err := d.Open(cfg)
	if err != nil {
		return nil, err
	}

	return &DB{conn: conn, engine: cfg.Engine}, nil
}

// Conn returns the underlying driver connection.
// Use this for engine-specific operations via type assertion:
//
//	if exp, ok := db.Conn().(db.Expirer); ok {
//	    exp.SetWithTTL(ctx, "sessions", sid, data, 24*time.Hour)
//	}
func (db *DB) Conn() Conn {
	return db.conn
}

// Engine returns the engine type of this connection.
func (db *DB) Engine() Engine {
	return db.engine
}

// Get retrieves a single record by its primary key.
func (db *DB) Get(ctx context.Context, collection string, id any, dest any) error {
	return db.conn.Get(ctx, collection, id, dest)
}

// Set creates or replaces a record.
func (db *DB) Set(ctx context.Context, collection string, id any, data any) error {
	return db.conn.Set(ctx, collection, id, data)
}

// Delete removes a record by its primary key.
func (db *DB) Delete(ctx context.Context, collection string, id any) (int64, error) {
	return db.conn.Delete(ctx, collection, id)
}

// Close closes the database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// Exec executes a query that doesn't return rows.
// Returns ErrNotSupported if the driver does not implement Querier.
func (db *DB) Exec(ctx context.Context, query string, args ...any) (Result, error) {
	q, ok := db.conn.(Querier)
	if !ok {
		return Result{}, ErrNotSupported
	}
	return q.Exec(ctx, query, args...)
}

// Query executes a query that returns rows.
// Returns ErrNotSupported if the driver does not implement Querier.
func (db *DB) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	q, ok := db.conn.(Querier)
	if !ok {
		return nil, ErrNotSupported
	}
	return q.Query(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row.
// Returns nil if the driver does not implement Querier.
func (db *DB) QueryRow(ctx context.Context, query string, args ...any) Row {
	q, ok := db.conn.(Querier)
	if !ok {
		return nil
	}
	return q.QueryRow(ctx, query, args...)
}

// Transaction executes fn within a transaction.
// If fn returns nil, the transaction is committed.
// If fn returns an error, the transaction is rolled back.
// Returns ErrNotSupported if the driver does not implement Transactioner.
func (db *DB) Transaction(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error {
	t, ok := db.conn.(Transactioner)
	if !ok {
		return ErrNotSupported
	}

	tx, err := t.Begin(ctx)
	if err != nil {
		return err
	}

	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// ExecBuilder implements builder.Executor for query builders.
func (db *DB) ExecBuilder(ctx context.Context, query string, args ...any) (builder.Result, error) {
	r, err := db.Exec(ctx, query, args...)
	if err != nil {
		return builder.Result{}, err
	}
	return builder.Result{
		RowsAffected: r.RowsAffected,
		LastInsertID: r.LastInsertID,
	}, nil
}

// ScanBuilder implements builder.Executor for query builders.
func (db *DB) ScanBuilder(ctx context.Context, dest any, query string, args ...any) error {
	_ = dest
	_ = query
	_ = args
	// TODO: Implement scan logic using Query + reflection.
	return ErrNotSupported
}

// NewSelect starts building a SELECT query.
func (db *DB) NewSelect(dest any) *builder.Select {
	return builder.NewSelect(db, dest)
}

// NewInsert starts building an INSERT query.
func (db *DB) NewInsert(model any) *builder.Insert {
	return builder.NewInsert(db, model)
}

// NewUpdate starts building an UPDATE query.
func (db *DB) NewUpdate(model any) *builder.Update {
	return builder.NewUpdate(db, model)
}

// NewDelete starts building a DELETE query.
func (db *DB) NewDelete(model any) *builder.Delete {
	return builder.NewDelete(db, model)
}
