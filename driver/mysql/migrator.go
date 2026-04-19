package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/migrate"
)

// Migrator implements the dialect.Migrator interface for MySQL.
type Migrator struct {
	driver *Driver
}

func (m *Migrator) AutoMigrate(models ...interface{}) error {
	inner := migrate.NewMigrator(&myDB{m.driver.db}, m.driver)
	return inner.AutoMigrate(context.Background(), models...)
}

func (m *Migrator) CreateTable(name string, models ...interface{}) error {
	if len(models) > 0 {
		return m.AutoMigrate(models...)
	}
	_, err := m.driver.db.Exec("CREATE TABLE IF NOT EXISTS " + quote(name) + " (id BIGINT AUTO_INCREMENT PRIMARY KEY)")
	return err
}

func (m *Migrator) DropTable(name string) error {
	_, err := m.driver.db.Exec("DROP TABLE IF EXISTS " + quote(name))
	return err
}

func (m *Migrator) HasTable(name string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		name,
	).Scan(&count)
	return count > 0
}

func (m *Migrator) RenameTable(oldName, newName string) error {
	_, err := m.driver.db.Exec("RENAME TABLE " + quote(oldName) + " TO " + quote(newName))
	return err
}

func (m *Migrator) GetTables() ([]string, error) {
	rows, err := m.driver.db.Query(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
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
		"SELECT count(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
		table, column,
	).Scan(&count)
	return count > 0
}

func (m *Migrator) AddColumn(table, column, columnType string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", quote(table), quote(column), columnType))
	return err
}

func (m *Migrator) DropColumn(table, column string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", quote(table), quote(column)))
	return err
}

func (m *Migrator) RenameColumn(table, oldName, newName string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", quote(table), quote(oldName), quote(newName)))
	return err
}

func (m *Migrator) AlterColumn(table, column, newType string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", quote(table), quote(column), newType))
	return err
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
	q += fmt.Sprintf("INDEX %s ON %s (%s)", quote(name), quote(table), strings.Join(quotedCols, ", "))
	_, err := m.driver.db.Exec(q)
	return err
}

func (m *Migrator) DropIndex(table, name string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("DROP INDEX %s ON %s", quote(name), quote(table)))
	return err
}

func (m *Migrator) HasIndex(table, name string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
		table, name,
	).Scan(&count)
	return count > 0
}

func (m *Migrator) GetIndexes(table string) ([]dialect.Index, error) {
	rows, err := m.driver.db.Query(
		"SELECT index_name, non_unique FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? GROUP BY index_name, non_unique",
		table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []dialect.Index
	for rows.Next() {
		var name string
		var nonUnique int
		if err := rows.Scan(&name, &nonUnique); err != nil {
			return nil, err
		}
		indexes = append(indexes, dialect.Index{Name: name, Unique: nonUnique == 0})
	}
	return indexes, rows.Err()
}

// quote wraps a MySQL identifier in backticks.
func quote(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "") + "`"
}

// myDB wraps *sql.DB to satisfy migrate.DB.
type myDB struct{ db *sql.DB }

func (s *myDB) Exec(ctx context.Context, q string, args ...interface{}) error {
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

func (s *myDB) Query(ctx context.Context, q string, args ...interface{}) (migrate.Result, error) {
	return s.db.QueryContext(ctx, q, args...)
}
