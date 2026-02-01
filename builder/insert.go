package builder

import "context"

// Insert builds an INSERT query using a fluent API.
//
//	user := User{Name: "John", Email: "john@example.com"}
//	result, err := db.NewInsert(&user).Exec(ctx)
type Insert struct {
	exec       Executor
	model      any
	columns    []string
	returning  []string
	onConflict string
}

// NewInsert creates a new InsertBuilder.
func NewInsert(exec Executor, model any) *Insert {
	return &Insert{exec: exec, model: model}
}

// Column specifies which columns to insert. If not called, all columns are used.
func (b *Insert) Column(columns ...string) *Insert {
	b.columns = append(b.columns, columns...)
	return b
}

// Returning specifies columns to return after insert (PostgreSQL, SQLite).
//
//	db.NewInsert(&user).Returning("id", "created_at").Exec(ctx)
func (b *Insert) Returning(columns ...string) *Insert {
	b.returning = append(b.returning, columns...)
	return b
}

// OnConflict sets the conflict resolution strategy.
//
//	db.NewInsert(&user).OnConflict("(email) DO UPDATE SET name = EXCLUDED.name").Exec(ctx)
func (b *Insert) OnConflict(clause string) *Insert {
	b.onConflict = clause
	return b
}

// Exec executes the INSERT query.
func (b *Insert) Exec(ctx context.Context) (Result, error) {
	// TODO: Build SQL from model/columns and delegate to executor.
	_ = ctx
	return Result{}, nil
}
