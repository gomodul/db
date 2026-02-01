package clause

// From from clause
type From struct {
	Tables []Table
	Joins  []Join
}

// Name from clause name
func (from From) Name() string {
	return "FROM"
}
