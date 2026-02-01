package clause

// Set represents a column-value pair for SET clauses in UPDATE statements.
type Set struct {
	Column string
	Value  any
}
