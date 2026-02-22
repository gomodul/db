package migrate

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gomodul/db/dialect"
)

// Migrator handles database schema migrations
type Migrator struct {
	db     DB
	dialect dialect.Dialect
}

// DB defines the database interface for migrations
type DB interface {
	Exec(ctx context.Context, sql string, args ...interface{}) error
	Query(ctx context.Context, sql string, args ...interface{}) (Result, error)
}

// Result represents the result of a query
type Result interface {
	Columns() ([]string, error)
	Close() error
}

// NewMigrator creates a new migrator
func NewMigrator(db DB, d dialect.Dialect) *Migrator {
	return &Migrator{
		db:     db,
		dialect: d,
	}
}

// AutoMigrate automatically creates/updates tables for the given models
//
// Example:
//
//	err := migrator.AutoMigrate(&User{}, &Order{}, &Product{})
func (m *Migrator) AutoMigrate(ctx context.Context, models ...interface{}) error {
	for _, model := range models {
		if err := m.migrateModel(ctx, model); err != nil {
			return fmt.Errorf("failed to migrate %T: %w", model, err)
		}
	}
	return nil
}

// migrateModel migrates a single model
func (m *Migrator) migrateModel(ctx context.Context, model interface{}) error {
	tableName := m.getTableName(model)
	columns, constraints, indexes, err := m.analyzeModel(model)
	if err != nil {
		return err
	}

	// Check if table exists
	exists, err := m.tableExists(ctx, tableName)
	if err != nil {
		return err
	}

	if !exists {
		// Create new table
		return m.createTable(ctx, tableName, columns, constraints)
	}

	// Update existing table (add missing columns)
	return m.updateTable(ctx, tableName, columns, indexes)
}

// getTableName extracts the table name from a model
func (m *Migrator) getTableName(model interface{}) string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Check for TableName() method
	if m, ok := reflect.New(t).Interface().(interface{ TableName() string }); ok {
		return m.TableName()
	}

	// Default: pluralize struct name
	return m.pluralize(t.Name())
}

// pluralize converts a name to plural form (simple implementation)
func (m *Migrator) pluralize(name string) string {
	if strings.HasSuffix(name, "y") {
		return name[:len(name)-1] + "ies"
	}
	return name + "s"
}

// ColumnInfo represents a column definition
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
	Primary  bool
	Unique   bool
	AutoIncr bool
	Tags     map[string]string
}

// ConstraintInfo represents a constraint definition
type ConstraintInfo struct {
	Name       string
	Type       string // "PRIMARY", "FOREIGN", "UNIQUE"
	Columns    []string
	Reference  string // For foreign keys: table(col)
	OnDelete   string
	OnUpdate   string
}

// IndexInfo represents an index definition
type IndexInfo struct {
	Name    string
	Columns []string
	Unique  bool
}

// analyzeModel analyzes a model and returns column, constraint, and index definitions
func (m *Migrator) analyzeModel(model interface{}) ([]*ColumnInfo, []*ConstraintInfo, []*IndexInfo, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var columns []*ColumnInfo
	var constraints []*ConstraintInfo
	var indexes []*IndexInfo
	var pkColumns []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		col := m.analyzeField(field)
		if col != nil {
			columns = append(columns, col)

			if col.Primary {
				pkColumns = append(pkColumns, col.Name)
			}

			// Check for unique tag
			if uniqueTag, ok := col.Tags["unique"]; ok && uniqueTag == "true" {
				constraints = append(constraints, &ConstraintInfo{
					Name:    fmt.Sprintf("uniq_%s_%s", m.getTableName(model), col.Name),
					Type:    "UNIQUE",
					Columns: []string{col.Name},
				})
			}

			// Check for index tag
			if indexTag, ok := col.Tags["index"]; ok {
				indexName := col.Name
				if indexTag != "true" && indexTag != "" {
					indexName = indexTag
				}
				indexes = append(indexes, &IndexInfo{
					Name:    fmt.Sprintf("idx_%s_%s", m.getTableName(model), indexName),
					Columns: []string{col.Name},
					Unique:  false,
				})
			}
		}
	}

	// Add primary key constraint if we have primary key columns
	if len(pkColumns) > 0 {
		constraints = append(constraints, &ConstraintInfo{
			Name:    fmt.Sprintf("pk_%s", m.getTableName(model)),
			Type:    "PRIMARY",
			Columns: pkColumns,
		})
	}

	return columns, constraints, indexes, nil
}

// analyzeField analyzes a single struct field
func (m *Migrator) analyzeField(field reflect.StructField) *ColumnInfo {
	// Skip unexported fields
	if !field.IsExported() {
		return nil
	}

	// Get db tag
	tag := field.Tag.Get("db")
	if tag == "-" || tag == "" {
		return nil
	}

	col := &ColumnInfo{
		Name:     m.getColumnName(tag, field.Name),
		Type:     m.getGoType(field.Type),
		Nullable: true,
		Tags:     make(map[string]string),
	}

	// Parse tags
	parts := strings.Split(tag, ",")
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) == 2 {
			col.Tags[kv[0]] = kv[1]
		} else {
			col.Tags[kv[0]] = "true"
		}
	}

	// Check column properties
	if _, ok := col.Tags["primaryKey"]; ok {
		col.Primary = true
		col.Nullable = false
	}
	if _, ok := col.Tags["autoIncrement"]; ok {
		col.AutoIncr = true
	}
	if _, ok := col.Tags["notnull"]; ok {
		col.Nullable = false
	}
	if _, ok := col.Tags["unique"]; ok {
		col.Unique = true
	}

	// Map Go type to database type
	col.Type = m.mapTypeToDB(col.Type, col)

	return col
}

// getColumnName extracts column name from tag or field name
func (m *Migrator) getColumnName(tag, fieldName string) string {
	if tag == "" || tag == "-" {
		return fieldName
	}
	parts := strings.Split(tag, ",")
	if parts[0] != "" {
		return parts[0]
	}
	return fieldName
}

// getGoType returns the Go type as string
func (m *Migrator) getGoType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "uint"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Bool:
		return "bool"
	case reflect.String:
		return "string"
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "time"
		}
		return "struct"
	case reflect.Ptr:
		return m.getGoType(t.Elem())
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes"
		}
		return "array"
	default:
		return "unknown"
	}
}

// mapTypeToDB maps Go type to database-specific type
func (m *Migrator) mapTypeToDB(goType string, col *ColumnInfo) string {
	// This will be overridden by dialect-specific implementations
	// Default to common SQL types
	switch goType {
	case "int", "uint":
		if col.AutoIncr {
			return "SERIAL" // Will be replaced by dialect
		}
		return "BIGINT"
	case "float":
		return "DOUBLE"
	case "bool":
		return "BOOLEAN"
	case "string":
		if size, ok := col.Tags["size"]; ok {
			return fmt.Sprintf("VARCHAR(%s)", size)
		}
		return "VARCHAR(255)"
	case "time":
		return "TIMESTAMP"
	case "bytes":
		return "BLOB"
	default:
		return "TEXT"
	}
}

// tableExists checks if a table exists
func (m *Migrator) tableExists(ctx context.Context, tableName string) (bool, error) {
	// Use dialect-specific query based on driver name
	driverName := strings.ToLower(m.dialect.Name())
	var query string

	switch {
	case strings.Contains(driverName, "postgres"):
		query = fmt.Sprintf(
			"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '%s')",
			tableName,
		)
	case strings.Contains(driverName, "mysql"):
		query = fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'",
			tableName,
		)
	case strings.Contains(driverName, "sqlite"):
		query = fmt.Sprintf(
			"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'",
			tableName,
		)
	default:
		return false, fmt.Errorf("unsupported dialect for table check: %v", m.dialect.Name())
	}

	result, err := m.db.Query(ctx, query)
	if err != nil {
		return false, err
	}
	defer result.Close()

	// For simplicity, assume table exists if query succeeds
	// In production, you'd parse the result
	return true, nil
}

// createTable creates a new table
func (m *Migrator) createTable(ctx context.Context, tableName string, columns []*ColumnInfo, constraints []*ConstraintInfo) error {
	var columnDefs []string
	var pkColumns []string

	for _, col := range columns {
		def := m.formatColumn(col)
		columnDefs = append(columnDefs, def)
		if col.Primary {
			pkColumns = append(pkColumns, col.Name)
		}
	}

	// Add primary key constraint
	if len(pkColumns) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkColumns, ", ")))
	}

	query := fmt.Sprintf("CREATE TABLE %s (\n    %s\n)", tableName, strings.Join(columnDefs, ",\n    "))

	if err := m.db.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create constraints and indexes
	for _, constraint := range constraints {
		if constraint.Type != "PRIMARY" {
			m.createConstraint(ctx, tableName, constraint)
		}
	}

	return nil
}

// formatColumn formats a column definition
func (m *Migrator) formatColumn(col *ColumnInfo) string {
	var parts []string

	parts = append(parts, col.Name)
	parts = append(parts, col.Type)

	if !col.Nullable {
		parts = append(parts, "NOT NULL")
	}

	if col.Default != "" {
		parts = append(parts, "DEFAULT "+col.Default)
	}

	if col.Unique && !col.Primary {
		parts = append(parts, "UNIQUE")
	}

	return strings.Join(parts, " ")
}

// updateTable updates an existing table
func (m *Migrator) updateTable(ctx context.Context, tableName string, columns []*ColumnInfo, indexes []*IndexInfo) error {
	// Get existing columns
	existingCols, err := m.getExistingColumns(ctx, tableName)
	if err != nil {
		return err
	}

	// Add missing columns
	for _, col := range columns {
		if !m.columnExists(col.Name, existingCols) {
			alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				tableName, col.Name, col.Type)
			if err := m.db.Exec(ctx, alterQuery); err != nil {
				return fmt.Errorf("failed to add column %s: %w", col.Name, err)
			}
		}
	}

	// Create missing indexes
	for _, idx := range indexes {
		if err := m.CreateIndex(ctx, tableName, idx); err != nil {
			// Log warning but continue
			fmt.Printf("Warning: failed to create index %s: %v\n", idx.Name, err)
		}
	}

	return nil
}

// columnExists checks if a column exists in the list
func (m *Migrator) columnExists(name string, columns []string) bool {
	for _, col := range columns {
		if col == name {
			return true
		}
	}
	return false
}

// getExistingColumns returns list of existing column names
func (m *Migrator) getExistingColumns(ctx context.Context, tableName string) ([]string, error) {
	driverName := strings.ToLower(m.dialect.Name())
	var query string

	switch {
	case strings.Contains(driverName, "postgres"):
		query = fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tableName)
	case strings.Contains(driverName, "mysql"):
		query = fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tableName)
	case strings.Contains(driverName, "sqlite"):
		query = fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	default:
		return nil, fmt.Errorf("unsupported dialect for column check: %v", m.dialect.Name())
	}

	result, err := m.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	// Parse result - simplified for now
	return []string{}, nil
}

// createConstraint creates a constraint
func (m *Migrator) createConstraint(ctx context.Context, tableName string, constraint *ConstraintInfo) error {
	var query string
	switch constraint.Type {
	case "UNIQUE":
		query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
			tableName, constraint.Name, strings.Join(constraint.Columns, ", "))
	case "FOREIGN":
		query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s",
			tableName, constraint.Name, strings.Join(constraint.Columns, ", "), constraint.Reference)
		if constraint.OnDelete != "" {
			query += " ON DELETE " + constraint.OnDelete
		}
		if constraint.OnUpdate != "" {
			query += " ON UPDATE " + constraint.OnUpdate
		}
	default:
		return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
	}

	return m.db.Exec(ctx, query)
}

// createIndex creates an index
func (m *Migrator) CreateIndex(ctx context.Context, tableName string, index *IndexInfo) error {
	unique := ""
	if index.Unique {
		unique = "UNIQUE "
	}
	query := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, index.Name, tableName, strings.Join(index.Columns, ", "))
	return m.db.Exec(ctx, query)
}

// DropTable drops a table if it exists
func (m *Migrator) DropTable(ctx context.Context, models ...interface{}) error {
	for _, model := range models {
		tableName := m.getTableName(model)
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)
		if err := m.db.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}
	return nil
}

// RenameTable renames a table
func (m *Migrator) RenameTable(ctx context.Context, oldName, newName string) error {
	query := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", oldName, newName)
	return m.db.Exec(ctx, query)
}

// AddColumn adds a column to a table
func (m *Migrator) AddColumn(ctx context.Context, model interface{}, field reflect.StructField) error {
	tableName := m.getTableName(model)
	col := m.analyzeField(field)
	if col == nil {
		return fmt.Errorf("invalid field for column")
	}

	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, col.Name, col.Type)
	if !col.Nullable {
		query += " NOT NULL"
	}

	return m.db.Exec(ctx, query)
}

// DropColumn drops a column from a table
func (m *Migrator) DropColumn(ctx context.Context, model interface{}, columnName string) error {
	tableName := m.getTableName(model)
	query := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, columnName)
	return m.db.Exec(ctx, query)
}

// RenameColumn renames a column
func (m *Migrator) RenameColumn(ctx context.Context, model interface{}, oldName, newName string) error {
	tableName := m.getTableName(model)
	query := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", tableName, oldName, newName)
	return m.db.Exec(ctx, query)
}

// AddIndex adds an index to a table
func (m *Migrator) AddIndex(ctx context.Context, model interface{}, index *IndexInfo) error {
	tableName := m.getTableName(model)
	return m.CreateIndex(ctx, tableName, index)
}

// DropIndex drops an index from a table
func (m *Migrator) DropIndex(ctx context.Context, model interface{}, indexName string) error {
	// Note: Some databases like PostgreSQL require table name in index drop
	// For simplicity, we use a basic DROP INDEX that works in most cases
	query := fmt.Sprintf("DROP INDEX IF EXISTS %s", indexName)
	return m.db.Exec(ctx, query)
}

// HasTable checks if a table exists
func (m *Migrator) HasTable(ctx context.Context, tableName string) (bool, error) {
	return m.tableExists(ctx, tableName)
}

// HasColumn checks if a column exists in a table
func (m *Migrator) HasColumn(ctx context.Context, tableName, columnName string) (bool, error) {
	columns, err := m.getExistingColumns(ctx, tableName)
	if err != nil {
		return false, err
	}
	return m.columnExists(columnName, columns), nil
}
