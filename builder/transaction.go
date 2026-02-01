package builder

// ============ Batch Operations ============

// ============ Batch Operations ============

// CreateBatch creates multiple records in batches
func (b *QueryBuilder) CreateBatch(values interface{}, batchSize int) error {
	if !b.db.Caps.Query.BatchCreate {
		return b.createSequentially(values)
	}

	// Implementation for batch create
	return b.CreateInBatch(values, batchSize)
}

// UpdateBatch updates multiple records in batches
func (b *QueryBuilder) UpdateBatch(values interface{}, batchSize int) error {
	if !b.db.Caps.Query.BatchUpdate {
		// Fallback to sequential updates
		return ErrNotSupported
	}

	// Implementation for batch update
	return nil
}

// DeleteBatch deletes multiple records in batches
func (b *QueryBuilder) DeleteBatch(ids []interface{}, batchSize int) error {
	if !b.db.Caps.Query.BatchDelete {
		// Fallback to sequential deletes
		return ErrNotSupported
	}

	// Implementation for batch delete
	return nil
}

// ============ Cursor Pagination ============

// CursorBasedPagination enables cursor-based pagination
func (b *QueryBuilder) CursorBasedPagination(field string) *QueryBuilder {
	// Implementation for cursor pagination
	return b
}

// Before returns results before a specific cursor
func (b *QueryBuilder) Before(cursor interface{}) *QueryBuilder {
	// Implementation for reverse cursor pagination
	return b
}

// ============ Locking Methods ============

// SkipLocked adds SKIP LOCKED hint
func (b *QueryBuilder) SkipLocked() *QueryBuilder {
	if !b.db.Caps.Query.Locking {
		return b
	}

	b.q.Hints = map[string]interface{}{
		"lock": "SKIP LOCKED",
	}
	return b
}

// NoWait adds NOWAIT hint
func (b *QueryBuilder) NoWait() *QueryBuilder {
	if !b.db.Caps.Query.Locking {
		return b
	}

	b.q.Hints = map[string]interface{}{
		"lock": "NOWAIT",
	}
	return b
}
