package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/migrate"
)

// Migrator implements the dialect.Migrator interface for SQLite.
type Migrator struct {
	driver *Driver
}

func (m *Migrator) AutoMigrate(models ...interface{}) error {
	inner := migrate.NewMigrator(&liteDB{m.driver.db}, m.driver)
	return inner.AutoMigrate(context.Background(), models...)
}

func (m *Migrator) CreateTable(name string, models ...interface{}) error {
	if len(models) > 0 {
		return m.AutoMigrate(models...)
	}
	_, err := m.driver.db.Exec("CREATE TABLE IF NOT EXISTS " + quote(name) + " (id INTEGER PRIMARY KEY AUTOINCREMENT)")
	return err
}

func (m *Migrator) DropTable(name string) error {
	_, err := m.driver.db.Exec("DROP TABLE IF EXISTS " + quote(name))
	return err
}

func (m *Migrator) HasTable(name string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
		name,
	).Scan(&count)
	return count > 0
}

func (m *Migrator) RenameTable(oldName, newName string) error {
	_, err := m.driver.db.Exec("ALTER TABLE " + quote(oldName) + " RENAME TO " + quote(newName))
	return err
}

func (m *Migrator) GetTables() ([]string, error) {
	rows, err := m.driver.db.Query(
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func (m *Migrator) HasColumn(table, column string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM pragma_table_info(?) WHERE name=?",
		table, column,
	).Scan(&count)
	return count > 0
}

func (m *Migrator) AddColumn(table, column, columnType string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", quote(table), quote(column), columnType))
	return err
}

// DropColumn is not natively supported in SQLite without recreating the table.
func (m *Migrator) DropColumn(table, column string) error {
	return fmt.Errorf("sqlite: dropping columns requires recreating the table")
}

func (m *Migrator) RenameColumn(table, oldName, newName string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", quote(table), quote(oldName), quote(newName)))
	return err
}

// AlterColumn is not natively supported in SQLite without recreating the table.
func (m *Migrator) AlterColumn(table, column, newType string) error {
	return fmt.Errorf("sqlite: altering column types requires recreating the table")
}

func (m *Migrator) CreateIndex(table, name string, columns []string, unique bool) error {
	q := "CREATE "
	if unique {
		q += "UNIQUE "
	}
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = quote(c)
	}
	q += fmt.Sprintf("INDEX IF NOT EXISTS %s ON %s (%s)", quote(name), quote(table), strings.Join(quotedCols, ", "))
	_, err := m.driver.db.Exec(q)
	return err
}

func (m *Migrator) DropIndex(table, name string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("DROP INDEX IF EXISTS %s", quote(name)))
	return err
}

func (m *Migrator) HasIndex(table, name string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?",
		name,
	).Scan(&count)
	return count > 0
}

func (m *Migrator) GetIndexes(table string) ([]dialect.Index, error) {
	rows, err := m.driver.db.Query(
		"SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?",
		table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []dialect.Index
	for rows.Next() {
		var name string
		var ddl sql.NullString
		if err := rows.Scan(&name, &ddl); err != nil {
			return nil, err
		}
		indexes = append(indexes, dialect.Index{
			Name:   name,
			Unique: strings.Contains(strings.ToUpper(ddl.String), "UNIQUE"),
		})
	}
	return indexes, rows.Err()
}

// quote wraps a SQLite identifier in double quotes.
func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, "") + `"`
}

// liteDB wraps *sql.DB to satisfy migrate.DB.
type liteDB struct{ db *sql.DB }

func (s *liteDB) Exec(ctx context.Context, q string, args ...interface{}) error {
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

func (s *liteDB) Query(ctx context.Context, q string, args ...interface{}) (migrate.Result, error) {
	return s.db.QueryContext(ctx, q, args...)
}
