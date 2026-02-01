// Package postgres provides a PostgreSQL driver for github.com/gomodul/db
// using pgx/v5 as the underlying connection pool.
//
// Import this package for its side effect of registering the driver:
//
//	import _ "github.com/gomodul/db/driver/postgres"
//
//	d, err := db.Open(db.Config{
//	    Engine: db.EnginePostgreSQL,
//	    DSN:    "postgres://user:pass@localhost:5432/dbname?sslmode=disable",
//	})
package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gomodul/db"
)

func init() {
	db.Register(db.EnginePostgreSQL, &Driver{})
}

// Driver implements db.Driver for PostgreSQL.
type Driver struct{}

// Open creates a new PostgreSQL connection pool.
func (d *Driver) Open(cfg db.Config) (db.Conn, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}

	if cfg.MaxOpenConns > 0 {
		poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		poolCfg.MinConns = int32(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.ConnMaxLifetime
	}
	if cfg.ConnMaxIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.ConnMaxIdleTime
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}

	return &Conn{pool: pool}, nil
}

// Conn implements db.Conn, db.Querier, and db.Transactioner for PostgreSQL.
type Conn struct {
	pool *pgxpool.Pool
}

// Get retrieves a single record by primary key.
// collection is the table name, id is the primary key value.
// dest must be a pointer to a struct.
func (c *Conn) Get(ctx context.Context, collection string, id any, dest any) error {
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE id = $1 LIMIT 1`,
		quoteIdent(collection),
	)

	rows, err := c.pool.Query(ctx, query, id)
	if err != nil {
		return fmt.Errorf("postgres: get: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("postgres: get: %w", err)
		}
		return db.ErrNotFound
	}

	if err := scanStruct(rows, dest); err != nil {
		return fmt.Errorf("postgres: get: %w", err)
	}

	return nil
}

// Set creates or replaces a record using INSERT ... ON CONFLICT (id) DO UPDATE.
func (c *Conn) Set(ctx context.Context, collection string, id any, data any) error {
	columns, values, err := structToColumnsValues(data)
	if err != nil {
		return fmt.Errorf("postgres: set: %w", err)
	}

	// Ensure id is included
	hasID := false
	for _, col := range columns {
		if col == "id" {
			hasID = true
			break
		}
	}
	if !hasID {
		columns = append([]string{"id"}, columns...)
		values = append([]any{id}, values...)
	}

	placeholders := make([]string, len(values))
	updates := make([]string, 0, len(columns))
	for i, col := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		if col != "id" {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(col), quoteIdent(col)))
		}
	}

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdent(col)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s`,
		quoteIdent(collection),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updates, ", "),
	)

	if _, err := c.pool.Exec(ctx, query, values...); err != nil {
		return fmt.Errorf("postgres: set: %w", mapError(err))
	}

	return nil
}

// Delete removes a record by primary key.
func (c *Conn) Delete(ctx context.Context, collection string, id any) (int64, error) {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE id = $1`,
		quoteIdent(collection),
	)

	tag, err := c.pool.Exec(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("postgres: delete: %w", err)
	}

	return tag.RowsAffected(), nil
}

// Close closes the connection pool.
func (c *Conn) Close() error {
	c.pool.Close()
	return nil
}

// Exec executes a query that doesn't return rows.
func (c *Conn) Exec(ctx context.Context, query string, args ...any) (db.Result, error) {
	tag, err := c.pool.Exec(ctx, query, args...)
	if err != nil {
		return db.Result{}, fmt.Errorf("postgres: exec: %w", mapError(err))
	}

	return db.Result{RowsAffected: tag.RowsAffected()}, nil
}

// Query executes a query that returns rows.
func (c *Conn) Query(ctx context.Context, query string, args ...any) (db.Rows, error) {
	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query: %w", mapError(err))
	}

	return &Rows{rows: rows}, nil
}

// QueryRow executes a query that returns at most one row.
func (c *Conn) QueryRow(ctx context.Context, query string, args ...any) db.Row {
	row := c.pool.QueryRow(ctx, query, args...)
	return &Row{row: row}
}

// Begin starts a new transaction.
func (c *Conn) Begin(ctx context.Context) (db.Tx, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin: %w", err)
	}

	return &Tx{tx: tx}, nil
}

// Pool returns the underlying pgxpool.Pool for advanced usage.
func (c *Conn) Pool() *pgxpool.Pool {
	return c.pool
}

// Rows wraps pgx.Rows to implement db.Rows.
type Rows struct {
	rows pgx.Rows
}

func (r *Rows) Next() bool             { return r.rows.Next() }
func (r *Rows) Scan(dest ...any) error { return r.rows.Scan(dest...) }
func (r *Rows) Close() error           { r.rows.Close(); return nil }
func (r *Rows) Err() error             { return r.rows.Err() }

// Row wraps pgx.Row to implement db.Row.
type Row struct {
	row pgx.Row
}

func (r *Row) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.ErrNotFound
	}
	return err
}

// Tx wraps pgx.Tx to implement db.Tx.
type Tx struct {
	tx pgx.Tx
}

func (t *Tx) Get(ctx context.Context, collection string, id any, dest any) error {
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE id = $1 LIMIT 1`,
		quoteIdent(collection),
	)

	rows, err := t.tx.Query(ctx, query, id)
	if err != nil {
		return fmt.Errorf("postgres: tx get: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("postgres: tx get: %w", err)
		}
		return db.ErrNotFound
	}

	return scanStruct(rows, dest)
}

func (t *Tx) Set(ctx context.Context, collection string, id any, data any) error {
	columns, values, err := structToColumnsValues(data)
	if err != nil {
		return fmt.Errorf("postgres: tx set: %w", err)
	}

	hasID := false
	for _, col := range columns {
		if col == "id" {
			hasID = true
			break
		}
	}
	if !hasID {
		columns = append([]string{"id"}, columns...)
		values = append([]any{id}, values...)
	}

	placeholders := make([]string, len(values))
	updates := make([]string, 0, len(columns))
	for i, col := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		if col != "id" {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(col), quoteIdent(col)))
		}
	}

	quotedCols := make([]string, len(columns))
	for i, col := range columns {
		quotedCols[i] = quoteIdent(col)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s`,
		quoteIdent(collection),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updates, ", "),
	)

	if _, err := t.tx.Exec(ctx, query, values...); err != nil {
		return fmt.Errorf("postgres: tx set: %w", mapError(err))
	}

	return nil
}

func (t *Tx) Delete(ctx context.Context, collection string, id any) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE id = $1`,
		quoteIdent(collection),
	)

	tag, err := t.tx.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("postgres: tx delete: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return db.ErrNotFound
	}

	return nil
}

func (t *Tx) Close() error { return nil }

func (t *Tx) Exec(ctx context.Context, query string, args ...any) (db.Result, error) {
	tag, err := t.tx.Exec(ctx, query, args...)
	if err != nil {
		return db.Result{}, fmt.Errorf("postgres: tx exec: %w", mapError(err))
	}
	return db.Result{RowsAffected: tag.RowsAffected()}, nil
}

func (t *Tx) Query(ctx context.Context, query string, args ...any) (db.Rows, error) {
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: tx query: %w", mapError(err))
	}
	return &Rows{rows: rows}, nil
}

func (t *Tx) QueryRow(ctx context.Context, query string, args ...any) db.Row {
	row := t.tx.QueryRow(ctx, query, args...)
	return &Row{row: row}
}

func (t *Tx) Commit() error {
	if err := t.tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

func (t *Tx) Rollback() error {
	if err := t.tx.Rollback(context.Background()); err != nil {
		return fmt.Errorf("postgres: rollback: %w", err)
	}
	return nil
}

// quoteIdent quotes a SQL identifier to prevent injection.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// mapError translates PostgreSQL-specific errors to db sentinel errors.
func mapError(err error) error {
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return fmt.Errorf("%w: %s", db.ErrDuplicate, pgErr.Message)
		}
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return db.ErrNotFound
	}

	return err
}

// scanStruct scans the current pgx row into a struct pointer using field descriptions.
func scanStruct(rows pgx.Rows, dest any) error {
	descs := rows.FieldDescriptions()
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("postgres: dest must be a pointer to a struct, got %T", dest)
	}

	rv = rv.Elem()
	rt := rv.Type()

	// Build column name -> field index mapping
	fieldMap := make(map[string]int, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag := f.Tag.Get("db")
		if tag == "-" {
			continue
		}
		name := tag
		if comma := strings.Index(tag, ","); comma != -1 {
			name = tag[:comma]
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		fieldMap[name] = i
	}

	// Build scan targets in column order
	scanTargets := make([]any, len(descs))
	for i, desc := range descs {
		colName := string(desc.Name)
		if idx, ok := fieldMap[colName]; ok {
			scanTargets[i] = rv.Field(idx).Addr().Interface()
		} else {
			// Discard unknown columns
			var discard any
			scanTargets[i] = &discard
		}
	}

	return rows.Scan(scanTargets...)
}

// structToColumnsValues extracts column names and values from a struct using db tags.
func structToColumnsValues(data any) ([]string, []any, error) {
	rv := reflect.ValueOf(data)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("postgres: data must be a struct, got %T", data)
	}

	rt := rv.Type()
	columns := make([]string, 0, rt.NumField())
	values := make([]any, 0, rt.NumField())

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)

		// Skip unexported fields
		if !f.IsExported() {
			continue
		}

		tag := f.Tag.Get("db")
		if tag == "-" {
			continue
		}

		name := tag
		if comma := strings.Index(tag, ","); comma != -1 {
			name = tag[:comma]
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}

		columns = append(columns, name)
		values = append(values, rv.Field(i).Interface())
	}

	return columns, values, nil
}
