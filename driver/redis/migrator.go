package redis

import (
	"github.com/gomodul/db/dialect"
)

// Migrator implements the dialect.Migrator interface for Redis.
// Redis has no schema concept; most operations return ErrNotSupported.
type Migrator struct {
	driver *Driver
}

func (m *Migrator) AutoMigrate(models ...interface{}) error  { return nil }
func (m *Migrator) CreateTable(name string, models ...interface{}) error { return nil }
func (m *Migrator) DropTable(name string) error              { return dialect.ErrNotSupported }
func (m *Migrator) HasTable(name string) bool                { return false }
func (m *Migrator) RenameTable(oldName, newName string) error { return dialect.ErrNotSupported }
func (m *Migrator) GetTables() ([]string, error)             { return nil, nil }
func (m *Migrator) AddColumn(table, column, columnType string) error { return dialect.ErrNotSupported }
func (m *Migrator) DropColumn(table, column string) error    { return dialect.ErrNotSupported }
func (m *Migrator) AlterColumn(table, column, newType string) error  { return dialect.ErrNotSupported }
func (m *Migrator) HasColumn(table, column string) bool      { return false }
func (m *Migrator) RenameColumn(table, oldName, newName string) error { return dialect.ErrNotSupported }
func (m *Migrator) CreateIndex(table, name string, columns []string, unique bool) error {
	return dialect.ErrNotSupported
}
func (m *Migrator) DropIndex(table, name string) error       { return dialect.ErrNotSupported }
func (m *Migrator) HasIndex(table, name string) bool         { return false }
func (m *Migrator) GetIndexes(table string) ([]dialect.Index, error) { return nil, nil }
