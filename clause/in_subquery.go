package clause

// InSubquery represents an IN clause with a subquery.
// Example: WHERE id IN (SELECT user_id FROM orders WHERE total > 1000)
type InSubquery struct {
	Column    string
	Query     string
	QueryArgs []any
}
