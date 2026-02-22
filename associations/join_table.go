package associations

import (
	"fmt"
	"reflect"
	"strings"
)

// JoinTable represents a many-to-many join table
type JoinTable struct {
	Name         string
	SourceFK     string
	TargetFK     string
	SourceModel  any
	TargetModel  any
	SourceRef    string
	TargetRef    string
}

// NewJoinTable creates a new join table definition
func NewJoinTable(name, sourceFK, targetFK string) *JoinTable {
	return &JoinTable{
		Name:     name,
		SourceFK: sourceFK,
		TargetFK: targetFK,
	}
}

// WithModels sets the source and target models
func (jt *JoinTable) WithModels(source, target any) *JoinTable {
	jt.SourceModel = source
	jt.TargetModel = target
	return jt
}

// WithRefs sets the source and target references
func (jt *JoinTable) WithRefs(sourceRef, targetRef string) *JoinTable {
	jt.SourceRef = sourceRef
	jt.TargetRef = targetRef
	return jt
}

// Manager manages join table operations
type Manager struct {
	joinTables map[string]*JoinTable
}

// NewManager creates a new join table manager
func NewManager() *Manager {
	return &Manager{
		joinTables: make(map[string]*JoinTable),
	}
}

// Register registers a join table
func (m *Manager) Register(joinTable *JoinTable) {
	m.joinTables[joinTable.Name] = joinTable
}

// Get retrieves a join table by name
func (m *Manager) Get(name string) (*JoinTable, bool) {
	jt, ok := m.joinTables[name]
	return jt, ok
}

// GenerateJoinTableName generates a join table name from two model names
func GenerateJoinTableName(model1, model2 string) string {
	names := []string{strings.ToLower(model1), strings.ToLower(model2)}

	// Sort alphabetically for consistency
	if names[0] > names[1] {
		names[0], names[1] = names[1], names[0]
	}

	return strings.Join(names, "_")
}

// ParseJoinTableTag parses a join table tag from struct field
//
// Tag format: "many2many:join_table:fk:assoc"
// Example: `db:"many2many:user_roles:user_id:role_id"`
func ParseJoinTableTag(tag string) (*JoinTable, error) {
	if !strings.Contains(tag, "many2many:") {
		return nil, fmt.Errorf("not a many2many tag")
	}

	// Extract the config part
	configStr := strings.TrimPrefix(tag, "many2many:")
	parts := strings.Split(configStr, ":")

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid many2many tag format, expected: many2many:join_table:fk:assoc")
	}

	jt := &JoinTable{
		Name:     parts[0],
		SourceFK: parts[1],
		TargetFK: parts[2],
	}

	return jt, nil
}

// JoinTableEntry represents an entry in a join table
type JoinTableEntry struct {
	SourceID any
	TargetID any
}

// AssociationData represents association data from join table
type AssociationData struct {
	JoinTable    string
	ForeignKey   string
	Association  string
	SourceID     any
	TargetIDs    []any
}

// BuildJoinQuery builds a query for loading many-to-many associations
func BuildJoinQuery(joinTable *JoinTable, sourceIDs []any) (string, []any, error) {
	if joinTable == nil {
		return "", nil, fmt.Errorf("join table is nil")
	}

	if len(sourceIDs) == 0 {
		return "", nil, fmt.Errorf("no source IDs provided")
	}

	// Build query
	query := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s IN (",
		joinTable.SourceFK,
		joinTable.TargetFK,
		joinTable.Name,
		joinTable.SourceFK,
	)

	// Add placeholders
	placeholders := make([]string, len(sourceIDs))
	args := make([]any, len(sourceIDs))
	for i, id := range sourceIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query += strings.Join(placeholders, ", ") + ")"

	return query, args, nil
}

// ExtractAssociationIDs extracts association IDs from join table results
func ExtractAssociationIDs(results []JoinTableEntry, sourceID any) []any {
	var ids []any

	for _, result := range results {
		if result.SourceID == sourceID {
			ids = append(ids, result.TargetID)
		}
	}

	return ids
}

// GetAssociationFieldNames returns the field names for an association
func GetAssociationFieldNames(model any, association string) (string, string, error) {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return "", "", fmt.Errorf("model must be a struct")
	}

	rt := rv.Type()

	// Find the association field
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		// Check field name
		if strings.EqualFold(field.Name, association) {
			return field.Name, field.Name, nil
		}

		// Check tags
		if tag := field.Tag.Get("json"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], association) {
					return field.Name, tag[:idx], nil
				}
			} else if strings.EqualFold(tag, association) {
				return field.Name, tag, nil
			}
		}

		if tag := field.Tag.Get("db"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], association) {
					return field.Name, tag[:idx], nil
				}
			} else if strings.EqualFold(tag, association) {
				return field.Name, tag, nil
			}
		}
	}

	return "", "", fmt.Errorf("association field %s not found", association)
}

// JoinTableSchema represents a join table schema
type JoinTableSchema struct {
	TableName   string
	SourceModel string
	TargetModel string
	SourceFK    string
	TargetFK    string
	Indexes     []string
}

// CreateJoinTableSQL generates SQL to create a join table
func CreateJoinTableSQL(schema *JoinTableSchema) string {
	sql := fmt.Sprintf("CREATE TABLE %s (", schema.TableName)

	// Add columns
	sql += fmt.Sprintf("%s BIGINT NOT NULL, ", schema.SourceFK)
	sql += fmt.Sprintf("%s BIGINT NOT NULL", schema.TargetFK)

	// Add primary key
	sql += fmt.Sprintf(", PRIMARY KEY (%s, %s)", schema.SourceFK, schema.TargetFK)

	sql += ")"

	return sql
}

// CreateJoinTableIndexSQL generates SQL to create indexes for a join table
func CreateJoinTableIndexSQL(schema *JoinTableSchema) []string {
	var indexes []string

	// Index on source FK
	indexes = append(indexes, fmt.Sprintf(
		"CREATE INDEX idx_%s_%s ON %s (%s)",
		schema.TableName, schema.SourceFK, schema.TableName, schema.SourceFK,
	))

	// Index on target FK
	indexes = append(indexes, fmt.Sprintf(
		"CREATE INDEX idx_%s_%s ON %s (%s)",
		schema.TableName, schema.TargetFK, schema.TableName, schema.TargetFK,
	))

	return indexes
}

// ValidateJoinTable validates a join table configuration
func ValidateJoinTable(jt *JoinTable) error {
	if jt == nil {
		return fmt.Errorf("join table is nil")
	}

	if jt.Name == "" {
		return fmt.Errorf("join table name is required")
	}

	if jt.SourceFK == "" {
		return fmt.Errorf("source foreign key is required")
	}

	if jt.TargetFK == "" {
		return fmt.Errorf("target foreign key is required")
	}

	return nil
}
