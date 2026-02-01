package schema

// Name namer interface
type Name interface {
	TableName(table string) string
	SchemaName(table string) string
	ColumnName(table, column string) string
	JoinTableName(joinTable string) string
	RelationshipFKName(Relationship) string
	CheckerName(table, column string) string
	IndexName(table, column string) string
	UniqueName(table, column string) string
}
