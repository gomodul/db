package query

// Order represents sorting configuration
type Order struct {
	Field     string
	Direction SortDirection
}

// SortDirection represents the sort direction
type SortDirection string

const (
	DirAsc  SortDirection = "ASC"
	DirDesc SortDirection = "DESC"
)

// Asc creates an ascending order
func Asc(field string) *Order {
	return &Order{
		Field:     field,
		Direction: DirAsc,
	}
}

// Desc creates a descending order
func Desc(field string) *Order {
	return &Order{
		Field:     field,
		Direction: DirDesc,
	}
}
