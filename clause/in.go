package clause

// In represents an IN clause for WHERE conditions.
// Example: WHERE id IN (1, 2, 3)
type In struct {
	Column string
	Values []any
}
