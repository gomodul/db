package clause

// Or represents an OR condition that can be combined with WHERE clauses.
// Example: WHERE age > 18 OR status = 'active'
//
//	Where(clause.Or{
//	    Conditions: []string{"age > ?", "status = ?"},
//	    Args: []any{18, "active"},
//	})
type Or struct {
	Conditions []string // Multiple conditions joined with OR
	Args       []any
}
