package db

import (
	"context"
	"database/sql"
)

// Client is a generic database client interface.
// This is used for NoSQL databases and API-based data sources that don't implement database/sql.
type Client interface {
	// Query executes a query and returns the results.
	// The implementation is database-specific.
	Query(ctx context.Context, query string, args ...interface{}) (Result, error)

	// Exec executes a command without returning any rows.
	Exec(ctx context.Context, query string, args ...interface{}) (Result, error)

	// Close closes the client connection.
	Close() error
}

// SQLClient wraps standard sql.DB for SQL databases
type SQLClient struct {
	DB *sql.DB
}

// Query executes a SQL query
func (c *SQLClient) Query(ctx context.Context, query string, args ...interface{}) (Result, error) {
	rows, err := c.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return Result{}, err
	}
	defer rows.Close()

	return Result{}, nil
}

// Exec executes a SQL command
func (c *SQLClient) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	res, err := c.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return Result{}, err
	}

	affected, _ := res.RowsAffected()
	lastID, _ := res.LastInsertId()

	return Result{
		RowsAffected: affected,
		LastInsertID: lastID,
	}, nil
}

// Close closes the database connection
func (c *SQLClient) Close() error {
	return c.DB.Close()
}

// PrepareContext prepares a statement
func (c *SQLClient) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.DB.PrepareContext(ctx, query)
}
