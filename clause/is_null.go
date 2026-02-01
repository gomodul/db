package clause

// IsNull represents an IS NULL clause.
// Example: WHERE deleted_at IS NULL
type IsNull struct {
	Column string
}

// IsNotNull represents an IS NOT NULL clause.
// Example: WHERE deleted_at IS NOT NULL
type IsNotNull struct {
	Column string
}
