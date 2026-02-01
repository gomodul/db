package clause

type OrderBy struct {
	Columns    []OrderByColumn
	Expression Expression
}

// Name where clause name
func (orderBy OrderBy) Name() string {
	return "ORDER BY"
}
