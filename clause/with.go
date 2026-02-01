package clause

// With represents a Common Table Expression (WITH clause).
// Example: WITH active_users AS (SELECT * FROM users WHERE status = 'active') SELECT * FROM active_users
type With struct {
	Name      string
	Columns   []string // Optional column names
	Query     string
	QueryArgs []any
	Recursive bool // Whether this is a recursive CTE (WITH RECURSIVE)
}
