package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/gomodul/db/dialect"
)

// Migrator implements the dialect.Migrator interface for MySQL
type Migrator struct {
	driver *Driver
}

// CurrentDatabase returns the current database name
func (m *Migrator) CurrentDatabase() string {
	var database sql.NullString
	m.driver.db.QueryRow("SELECT DATABASE()").Scan(&database)
	return database.String
}

// CreateTable creates a new table
func (m *Migrator) CreateTable(table string, columns []dialect.ColumnDefinition) error {
	var createSQL strings.Builder

	createSQL.WriteString("CREATE TABLE ")
	createSQL.WriteString(table)
	createSQL.WriteString(" (")

	for i, col := range columns {
		if i > 0 {
			createSQL.WriteString(", ")
		}

		createSQL.WriteString(col.Name)
		createSQL.WriteString(" ")
		createSQL.WriteString(col.Type)

		if col.PrimaryKey {
			createSQL.WriteString(" PRIMARY KEY")
		}
		if col.NotNull {
			createSQL.WriteString(" NOT NULL")
		}
		if col.Unique {
			createSQL.WriteString(" UNIQUE")
		}
		if col.Default != nil {
			createSQL.WriteString(" DEFAULT ")
			createSQL.WriteString(fmt.Sprintf("%v", col.Default))
		}
	}

	createSQL.WriteString(")")

	_, err := m.driver.db.Exec(createSQL.String())
	return err
}

// DropTable drops a table
func (m *Migrator) DropTable(table string) error {
	sql := "DROP TABLE " + table
	_, err := m.driver.db.Exec(sql)
	return err
}

// RenameTable renames a table
func (m *Migrator) RenameTable(oldName, newName string) error {
	_, err := m.driver.db.Exec("ALTER TABLE " + oldName + " RENAME TO " + newName)
	return err
}

// HasTable checks if a table exists
func (m *Migrator) HasTable(table string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table,
	).Scan(&count)
	return count > 0
}

// GetTables returns all table names
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

	return tables, nil
}

// HasColumn checks if a column exists in a table
func (m *Migrator) HasColumn(table, column string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
		table,
		column,
	).Scan(&count)
	return count > 0
}

// AddColumn adds a column to a table
func (m *Migrator) AddColumn(table, column, columnType string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, columnType))
	return err
}

// DropColumn drops a column from a table
func (m *Migrator) DropColumn(table, column string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, column))
	return err
}

// RenameColumn renames a column
func (m *Migrator) RenameColumn(table, oldName, newName string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName))
	return err
}

// AlterColumn alters a column type
func (m *Migrator) AlterColumn(table, column, newType string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, column, newType))
	return err
}

// CreateIndex creates an index
func (m *Migrator) CreateIndex(table, name string, columns []string, unique bool) error {
	sql := "CREATE "
	if unique {
		sql += "UNIQUE "
	}
	sql += fmt.Sprintf("INDEX %s ON %s (%s)", name, table, strings.Join(columns, ", "))
	_, err := m.driver.db.Exec(sql)
	return err
}

// DropIndex drops an index
func (m *Migrator) DropIndex(table, name string) error {
	_, err := m.driver.db.Exec(fmt.Sprintf("DROP INDEX %s", name))
	return err
}

// HasIndex checks if an index exists
func (m *Migrator) HasIndex(table, name string) bool {
	var count int64
	m.driver.db.QueryRow(
		"SELECT count(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
		table,
		name,
	).Scan(&count)
	return count > 0
}

// AutoMigrate creates tables based on models
func (m *Migrator) AutoMigrate(models ...interface{}) error {
	// TODO: Implement model introspection and table creation
	return nil
}
