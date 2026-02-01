package builder

import (
	"context"

	"github.com/gomodul/db/clause"
)

// Delete builds a DELETE query using a fluent API.
//
//	result, err := db.NewDelete(&User{}).
//	    Where("id = ?", 1).
//	    Exec(ctx)
type Delete struct {
	exec   Executor
	model  any
	wheres []clause.Where
	force  bool
}

// NewDelete creates a new DeleteBuilder.
func NewDelete(exec Executor, model any) *Delete {
	return &Delete{exec: exec, model: model}
}

// Where adds a WHERE condition.
// Multiple calls are joined with AND.
func (b *Delete) Where(condition string, args ...any) *Delete {
	b.wheres = append(b.wheres, clause.Where{Condition: condition, Args: args})
	return b
}

// WherePK adds a WHERE condition using the model's primary key.
func (b *Delete) WherePK() *Delete {
	b.wheres = append(b.wheres, clause.Where{Condition: "_pk_", Args: nil})
	return b
}

// Force bypasses soft delete and performs a hard delete.
func (b *Delete) Force() *Delete {
	b.force = true
	return b
}

// Exec executes the DELETE query.
func (b *Delete) Exec(ctx context.Context) (Result, error) {
	// TODO: Build SQL from wheres and delegate to executor.
	_ = ctx
	return Result{}, nil
}
