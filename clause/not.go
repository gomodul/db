package clause

// Not represents a NOT condition for WHERE clauses.
// Example: WHERE NOT status = 'deleted'
type Not struct {
	Condition string
	Args      []any
}
