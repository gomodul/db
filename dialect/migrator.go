package dialect

// Migrator is the universal schema migration interface.
// SQL drivers use table/column terminology.
// NoSQL drivers (MongoDB, Elasticsearch, Redis) map their equivalents to the same methods.
// Methods that don't apply to a backend should return ErrNotSupported.
type Migrator interface {
	// AutoMigrate creates or updates the schema for the given models.
	AutoMigrate(models ...interface{}) error

	// CreateTable creates a new table or collection.
	// SQL drivers infer column definitions from model struct tags.
	CreateTable(name string, models ...interface{}) error
	// DropTable drops a table or collection.
	DropTable(name string) error
	// HasTable reports whether a table or collection exists.
	HasTable(name string) bool
	// RenameTable renames a table or collection.
	RenameTable(oldName, newName string) error
	// GetTables returns all table or collection names.
	GetTables() ([]string, error)

	// AddColumn adds a column to a table (NoSQL drivers may ignore columnType).
	AddColumn(table, column, columnType string) error
	// DropColumn drops a column from a table.
	DropColumn(table, column string) error
	// AlterColumn changes a column's type (NoSQL drivers may ignore newType).
	AlterColumn(table, column, newType string) error
	// HasColumn reports whether a column or field exists.
	HasColumn(table, column string) bool
	// RenameColumn renames a column or field.
	RenameColumn(table, oldName, newName string) error

	// CreateIndex creates an index.
	CreateIndex(table, name string, columns []string, unique bool) error
	// DropIndex drops an index.
	DropIndex(table, name string) error
	// HasIndex reports whether an index exists.
	HasIndex(table, name string) bool
	// GetIndexes returns all indexes for a table or collection.
	GetIndexes(table string) ([]Index, error)
}

// Index represents a database index.
type Index struct {
	Name       string
	Fields     []string
	Unique     bool
	Primary    bool
	Sparsity   float64
	Filter     string
	Constraint string
}

// NoOpMigrator implements Migrator with no-op stubs.
// Embed or use directly for backends that don't support schema management.
type NoOpMigrator struct{}

func (m *NoOpMigrator) AutoMigrate(models ...interface{}) error                         { return nil }
func (m *NoOpMigrator) CreateTable(name string, models ...interface{}) error            { return nil }
func (m *NoOpMigrator) DropTable(name string) error                                     { return nil }
func (m *NoOpMigrator) HasTable(name string) bool                                       { return false }
func (m *NoOpMigrator) RenameTable(oldName, newName string) error                       { return nil }
func (m *NoOpMigrator) GetTables() ([]string, error)                                    { return nil, nil }
func (m *NoOpMigrator) AddColumn(table, column, columnType string) error                { return nil }
func (m *NoOpMigrator) DropColumn(table, column string) error                           { return nil }
func (m *NoOpMigrator) AlterColumn(table, column, newType string) error                 { return nil }
func (m *NoOpMigrator) HasColumn(table, column string) bool                             { return false }
func (m *NoOpMigrator) RenameColumn(table, oldName, newName string) error               { return nil }
func (m *NoOpMigrator) CreateIndex(table, name string, cols []string, unique bool) error { return nil }
func (m *NoOpMigrator) DropIndex(table, name string) error                              { return nil }
func (m *NoOpMigrator) HasIndex(table, name string) bool                                { return false }
func (m *NoOpMigrator) GetIndexes(table string) ([]Index, error)                        { return nil, nil }
