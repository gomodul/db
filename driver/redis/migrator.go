package redis

import (
	"github.com/gomodul/db/dialect"
)

// Migrator implements the dialect.Migrator interface for Redis
type Migrator struct {
	driver *Driver
}

// AutoMigrate is a no-op for Redis (schemaless)
func (m *Migrator) AutoMigrate(models ...interface{}) error {
	// Redis is schemaless, no migration needed
	return nil
}

// CreateCollection is a no-op for Redis (no collections)
func (m *Migrator) CreateCollection(name string, models ...interface{}) error {
	// Redis doesn't have collections
	return nil
}

// DropCollection is a no-op for Redis (no collections)
func (m *Migrator) DropCollection(name string) error {
	// Redis doesn't have collections
	return nil
}

// HasCollection returns false for Redis (no collections)
func (m *Migrator) HasCollection(name string) bool {
	// Redis doesn't have collections
	return false
}

// RenameCollection returns error for Redis (no collections)
func (m *Migrator) RenameCollection(oldName, newName string) error {
	return dialect.ErrNotSupported
}

// CreateIndex returns error for Redis (no indexes)
func (m *Migrator) CreateIndex(collection, name string, fields []string, unique bool) error {
	return dialect.ErrNotSupported
}

// DropIndex returns error for Redis (no indexes)
func (m *Migrator) DropIndex(collection, name string) error {
	return dialect.ErrNotSupported
}

// HasIndex returns false for Redis (no indexes)
func (m *Migrator) HasIndex(collection, name string) bool {
	return false
}

// GetIndexes returns nil for Redis (no indexes)
func (m *Migrator) GetIndexes(collection string) ([]dialect.Index, error) {
	return nil, nil
}

// AddColumn returns error for Redis (no columns)
func (m *Migrator) AddColumn(collection, field string) error {
	return dialect.ErrNotSupported
}

// DropColumn returns error for Redis (no columns)
func (m *Migrator) DropColumn(collection, field string) error {
	return dialect.ErrNotSupported
}

// AlterColumn returns error for Redis (no columns)
func (m *Migrator) AlterColumn(collection, field string) error {
	return dialect.ErrNotSupported
}
