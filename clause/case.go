package clause

// Case represents a CASE WHEN expression.
// Example: CASE WHEN status = 'active' THEN 1 ELSE 0 END
type Case struct {
	Conditions []CaseWhen
	ElseValue  any
	Alias      string // Optional alias for the CASE expression
}

// CaseWhen represents a single WHEN condition in CASE expression.
type CaseWhen struct {
	When string // WHEN condition
	Then any    // THEN value
}
