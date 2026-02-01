package clause

// Column quote with name
type Column struct {
	Table string
	Name  string
	Alias string
	Raw   bool
}
