package clause

// Locking represents a FOR UPDATE clause for row locking.
// Example: SELECT ... FOR UPDATE
type Locking struct {
	Strength string // "" = UPDATE, "SHARE", "NOWAIT", "SKIP LOCKED"
	Tables   []string // Optional: specific tables to lock
}
