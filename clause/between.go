package clause

// Between represents a BETWEEN clause for WHERE conditions.
// Example: WHERE age BETWEEN 18 AND 65
type Between struct {
	Column string
	Min    any
	Max    any
}
