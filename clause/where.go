package clause

// Where represents a single WHERE condition with its arguments.
type Where struct {
	Condition string
	Args      []any
}
