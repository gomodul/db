package dialect

// Migrator interface for schema management
type Migrator interface {
	// AutoMigrate automatically migrates schemas
	AutoMigrate(models ...interface{}) error

	// CreateCollection creates a new collection/table
	CreateCollection(name string, models ...interface{}) error

	// DropCollection drops a collection/table
	DropCollection(name string) error

	// HasCollection checks if a collection/table exists
	HasCollection(name string) bool

	// RenameCollection renames a collection/table
	RenameCollection(oldName, newName string) error

	// CreateIndex creates an index
	CreateIndex(collection, name string, fields []string, unique bool) error

	// DropIndex drops an index
	DropIndex(collection, name string) error

	// HasIndex checks if an index exists
	HasIndex(collection, name string) bool

	// GetIndexes returns all indexes for a collection
	GetIndexes(collection string) ([]Index, error)

	// AlterColumn alters a column
	AlterColumn(collection, field string) error

	// AddColumn adds a column
	AddColumn(collection, field string) error

	// DropColumn drops a column
	DropColumn(collection, field string) error

	// HasColumn checks if a column exists
	HasColumn(collection, field string) bool
}

// Index represents a database index
type Index struct {
	Name       string
	Fields     []string
	Unique     bool
	Primary    bool
	Sparsity   float64 // For partial indexes
	Filter     string  // For partial indexes
	Constraint string  // For constraint indexes
}

// NoOpMigrator is a migrator that does nothing (for databases that don't support schema migration)
type NoOpMigrator struct{}

// ColumnDefinition represents a column definition for table creation
type ColumnDefinition struct {
	Name      string
	Type      string
	NotNull   bool
	PrimaryKey bool
	Unique    bool
	Default   interface{}
}

// AutoMigrate does nothing
func (m *NoOpMigrator) AutoMigrate(models ...interface{}) error {
	return nil
}

// CreateCollection does nothing
func (m *NoOpMigrator) CreateCollection(name string, models ...interface{}) error {
	return nil
}

// DropCollection does nothing
func (m *NoOpMigrator) DropCollection(name string) error {
	return nil
}

// HasCollection returns false
func (m *NoOpMigrator) HasCollection(name string) bool {
	return false
}

// RenameCollection does nothing
func (m *NoOpMigrator) RenameCollection(oldName, newName string) error {
	return nil
}

// CreateIndex does nothing
func (m *NoOpMigrator) CreateIndex(collection, name string, fields []string, unique bool) error {
	return nil
}

// DropIndex does nothing
func (m *NoOpMigrator) DropIndex(collection, name string) error {
	return nil
}

// HasIndex returns false
func (m *NoOpMigrator) HasIndex(collection, name string) bool {
	return false
}

// GetIndexes returns nil
func (m *NoOpMigrator) GetIndexes(collection string) ([]Index, error) {
	return nil, nil
}

// AlterColumn does nothing
func (m *NoOpMigrator) AlterColumn(collection, field string) error {
	return nil
}

// AddColumn does nothing
func (m *NoOpMigrator) AddColumn(collection, field string) error {
	return nil
}

// DropColumn does nothing
func (m *NoOpMigrator) DropColumn(collection, field string) error {
	return nil
}

// HasColumn returns false
func (m *NoOpMigrator) HasColumn(collection, field string) bool {
	return false
}
