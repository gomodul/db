package migrate

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// DB is the minimal database interface required for migrations.
type DB interface {
	Exec(ctx context.Context, sql string, args ...interface{}) error
	Query(ctx context.Context, sql string, args ...interface{}) (Result, error)
}

// Result is the row-result interface, satisfied by *sql.Rows.
type Result interface {
	Next() bool
	Scan(dest ...interface{}) error
	Columns() ([]string, error)
	Close() error
}

// Dialect is the minimal dialect interface required for migrations.
type Dialect interface {
	Name() string
}

// Migrator handles database schema migrations.
type Migrator struct {
	db      DB
	dialect Dialect
}

// NewMigrator creates a new Migrator.
func NewMigrator(db DB, d Dialect) *Migrator {
	return &Migrator{db: db, dialect: d}
}

// AutoMigrate creates/updates tables for the given models.
func (m *Migrator) AutoMigrate(ctx context.Context, models ...interface{}) error {
	for _, model := range models {
		if err := m.migrateModel(ctx, model); err != nil {
			return fmt.Errorf("failed to migrate %T: %w", model, err)
		}
	}
	return nil
}

func (m *Migrator) migrateModel(ctx context.Context, model interface{}) error {
	tableName := m.getTableName(model)
	columns, constraints, indexes, err := m.analyzeModel(model)
	if err != nil {
		return err
	}

	exists, err := m.tableExists(ctx, tableName)
	if err != nil {
		return err
	}

	if !exists {
		return m.createTable(ctx, tableName, columns, constraints, indexes)
	}
	return m.updateTable(ctx, tableName, columns, indexes)
}

func (m *Migrator) getTableName(model interface{}) string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if tn, ok := reflect.New(t).Interface().(interface{ TableName() string }); ok {
		return tn.TableName()
	}
	return pluralize(t.Name())
}

func pluralize(name string) string {
	if strings.HasSuffix(name, "y") {
		return name[:len(name)-1] + "ies"
	}
	return name + "s"
}

// ColumnInfo represents a column definition.
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

// ConstraintInfo represents a constraint definition.
type ConstraintInfo struct {
	Name      string
	Type      string // "PRIMARY", "FOREIGN", "UNIQUE"
	Columns   []string
	Reference string
	OnDelete  string
	OnUpdate  string
}

// IndexInfo represents an index definition.
type IndexInfo struct {
	Name    string
	Columns []string
	Unique  bool
}

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
		col := m.analyzeField(t.Field(i))
		if col == nil {
			continue
		}
		columns = append(columns, col)

		if col.Primary {
			pkColumns = append(pkColumns, col.Name)
		}
		if col.Tags["unique"] == "true" {
			constraints = append(constraints, &ConstraintInfo{
				Name:    fmt.Sprintf("uniq_%s_%s", m.getTableName(model), col.Name),
				Type:    "UNIQUE",
				Columns: []string{col.Name},
			})
		}
		if indexTag, ok := col.Tags["index"]; ok {
			indexName := col.Name
			if indexTag != "true" && indexTag != "" {
				indexName = indexTag
			}
			indexes = append(indexes, &IndexInfo{
				Name:    fmt.Sprintf("idx_%s_%s", m.getTableName(model), indexName),
				Columns: []string{col.Name},
			})
		}
	}

	if len(pkColumns) > 0 {
		constraints = append(constraints, &ConstraintInfo{
			Name:    fmt.Sprintf("pk_%s", m.getTableName(model)),
			Type:    "PRIMARY",
			Columns: pkColumns,
		})
	}

	return columns, constraints, indexes, nil
}

func (m *Migrator) analyzeField(field reflect.StructField) *ColumnInfo {
	if !field.IsExported() {
		return nil
	}
	tag := field.Tag.Get("db")
	if tag == "-" {
		return nil
	}

	col := &ColumnInfo{
		Name:     columnNameFromTag(tag, field.Name),
		Type:     goTypeToString(field.Type),
		Nullable: true,
		Tags:     make(map[string]string),
	}

	parts := strings.Split(tag, ",")
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) == 2 {
			col.Tags[kv[0]] = kv[1]
		} else if kv[0] != "" {
			col.Tags[kv[0]] = "true"
		}
	}

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

	col.Type = m.mapTypeToDB(col.Type, col)
	return col
}

func columnNameFromTag(tag, fieldName string) string {
	parts := strings.Split(tag, ",")
	if parts[0] != "" {
		return parts[0]
	}
	return strings.ToLower(fieldName)
}

func goTypeToString(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
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
		return goTypeToString(t.Elem())
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "bytes"
		}
		return "array"
	default:
		return "unknown"
	}
}

func (m *Migrator) mapTypeToDB(goType string, col *ColumnInfo) string {
	driverName := strings.ToLower(m.dialect.Name())
	switch goType {
	case "int":
		if col.AutoIncr {
			if strings.Contains(driverName, "mysql") {
				return "BIGINT AUTO_INCREMENT"
			}
			if strings.Contains(driverName, "postgres") {
				return "BIGSERIAL"
			}
			return "INTEGER"
		}
		return "BIGINT"
	case "float":
		return "DOUBLE PRECISION"
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

// quote wraps an identifier in dialect-appropriate quote characters.
func (m *Migrator) quote(name string) string {
	if strings.Contains(strings.ToLower(m.dialect.Name()), "mysql") {
		return "`" + strings.ReplaceAll(name, "`", "") + "`"
	}
	return `"` + strings.ReplaceAll(name, `"`, "") + `"`
}

func (m *Migrator) tableExists(ctx context.Context, tableName string) (bool, error) {
	if m.db == nil {
		return false, nil
	}

	driverName := strings.ToLower(m.dialect.Name())
	var query string
	var args []interface{}

	switch {
	case strings.Contains(driverName, "postgres"):
		query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA() AND table_name = $1"
		args = []interface{}{tableName}
	case strings.Contains(driverName, "mysql"):
		query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?"
		args = []interface{}{tableName}
	case strings.Contains(driverName, "sqlite"):
		query = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
		args = []interface{}{tableName}
	default:
		return false, fmt.Errorf("unsupported dialect for table check: %s", m.dialect.Name())
	}

	rows, err := m.db.Query(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		var count int64
		if err := rows.Scan(&count); err != nil {
			return false, err
		}
		return count > 0, nil
	}
	return false, nil
}

func (m *Migrator) createTable(ctx context.Context, tableName string, columns []*ColumnInfo, constraints []*ConstraintInfo, indexes []*IndexInfo) error {
	var columnDefs []string
	var pkColumns []string

	for _, col := range columns {
		columnDefs = append(columnDefs, m.formatColumn(col))
		if col.Primary {
			pkColumns = append(pkColumns, m.quote(col.Name))
		}
	}
	if len(pkColumns) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkColumns, ", ")))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)", m.quote(tableName), strings.Join(columnDefs, ",\n    "))
	if err := m.db.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	for _, constraint := range constraints {
		if constraint.Type == "PRIMARY" {
			continue
		}
		if err := m.createConstraint(ctx, tableName, constraint); err != nil {
			return fmt.Errorf("failed to create constraint %s: %w", constraint.Name, err)
		}
	}

	for _, idx := range indexes {
		if err := m.CreateIndex(ctx, tableName, idx); err != nil {
			return fmt.Errorf("failed to create index %s: %w", idx.Name, err)
		}
	}

	return nil
}

func (m *Migrator) formatColumn(col *ColumnInfo) string {
	var parts []string
	parts = append(parts, m.quote(col.Name), col.Type)
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

func (m *Migrator) updateTable(ctx context.Context, tableName string, columns []*ColumnInfo, indexes []*IndexInfo) error {
	for _, col := range columns {
		if m.hasColumn(ctx, tableName, col.Name) {
			continue
		}
		alterQuery := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			m.quote(tableName), m.quote(col.Name), col.Type)
		if err := m.db.Exec(ctx, alterQuery); err != nil {
			return fmt.Errorf("failed to add column %s: %w", col.Name, err)
		}
	}

	for _, idx := range indexes {
		_ = m.CreateIndex(ctx, tableName, idx) // best-effort: index may already exist
	}

	return nil
}

func (m *Migrator) hasColumn(ctx context.Context, tableName, colName string) bool {
	if m.db == nil {
		return false
	}

	driverName := strings.ToLower(m.dialect.Name())
	var query string
	var args []interface{}

	switch {
	case strings.Contains(driverName, "postgres"):
		query = "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA() AND table_name = $1 AND column_name = $2"
		args = []interface{}{tableName, colName}
	case strings.Contains(driverName, "mysql"):
		query = "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?"
		args = []interface{}{tableName, colName}
	case strings.Contains(driverName, "sqlite"):
		query = "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?"
		args = []interface{}{tableName, colName}
	default:
		return false
	}

	rows, err := m.db.Query(ctx, query, args...)
	if err != nil {
		return false
	}
	defer rows.Close()

	if rows.Next() {
		var count int64
		_ = rows.Scan(&count)
		return count > 0
	}
	return false
}

func (m *Migrator) createConstraint(ctx context.Context, tableName string, constraint *ConstraintInfo) error {
	quotedCols := make([]string, len(constraint.Columns))
	for i, c := range constraint.Columns {
		quotedCols[i] = m.quote(c)
	}

	var query string
	switch constraint.Type {
	case "UNIQUE":
		query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
			m.quote(tableName), m.quote(constraint.Name), strings.Join(quotedCols, ", "))
	case "FOREIGN":
		query = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s",
			m.quote(tableName), m.quote(constraint.Name), strings.Join(quotedCols, ", "), constraint.Reference)
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

// CreateIndex creates an index on a table.
func (m *Migrator) CreateIndex(ctx context.Context, tableName string, index *IndexInfo) error {
	unique := ""
	if index.Unique {
		unique = "UNIQUE "
	}
	quotedCols := make([]string, len(index.Columns))
	for i, c := range index.Columns {
		quotedCols[i] = m.quote(c)
	}
	query := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique, m.quote(index.Name), m.quote(tableName), strings.Join(quotedCols, ", "))
	return m.db.Exec(ctx, query)
}

// DropTable drops tables derived from the given models.
func (m *Migrator) DropTable(ctx context.Context, models ...interface{}) error {
	for _, model := range models {
		tableName := m.getTableName(model)
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.quote(tableName))
		if err := m.db.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}
	return nil
}

// RenameTable renames a table.
func (m *Migrator) RenameTable(ctx context.Context, oldName, newName string) error {
	query := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", m.quote(oldName), m.quote(newName))
	return m.db.Exec(ctx, query)
}

// AddColumn adds a column derived from a struct field.
func (m *Migrator) AddColumn(ctx context.Context, model interface{}, field reflect.StructField) error {
	tableName := m.getTableName(model)
	col := m.analyzeField(field)
	if col == nil {
		return fmt.Errorf("field %s is not a valid column", field.Name)
	}
	query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", m.quote(tableName), m.quote(col.Name), col.Type)
	if !col.Nullable {
		query += " NOT NULL"
	}
	return m.db.Exec(ctx, query)
}

// DropColumn drops a column from a table.
func (m *Migrator) DropColumn(ctx context.Context, model interface{}, columnName string) error {
	tableName := m.getTableName(model)
	query := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", m.quote(tableName), m.quote(columnName))
	return m.db.Exec(ctx, query)
}

// RenameColumn renames a column.
func (m *Migrator) RenameColumn(ctx context.Context, model interface{}, oldName, newName string) error {
	tableName := m.getTableName(model)
	query := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s",
		m.quote(tableName), m.quote(oldName), m.quote(newName))
	return m.db.Exec(ctx, query)
}

// HasTable reports whether a table exists.
func (m *Migrator) HasTable(ctx context.Context, tableName string) (bool, error) {
	return m.tableExists(ctx, tableName)
}

// HasColumn reports whether a column exists.
func (m *Migrator) HasColumn(ctx context.Context, tableName, columnName string) bool {
	return m.hasColumn(ctx, tableName, columnName)
}

// AddIndex adds an index derived from a model.
func (m *Migrator) AddIndex(ctx context.Context, model interface{}, index *IndexInfo) error {
	return m.CreateIndex(ctx, m.getTableName(model), index)
}

// DropIndex drops an index by name.
func (m *Migrator) DropIndex(ctx context.Context, indexName string) error {
	query := fmt.Sprintf("DROP INDEX IF EXISTS %s", m.quote(indexName))
	return m.db.Exec(ctx, query)
}
