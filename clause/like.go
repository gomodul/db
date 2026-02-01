package clause

// Like represents a LIKE clause for WHERE conditions.
// Example: WHERE name LIKE '%john%'
type Like struct {
	Column  string
	Pattern string
}
