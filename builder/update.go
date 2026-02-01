package builder

import (
	"context"

	"github.com/gomodul/db/clause"
)

// Update builds an UPDATE query using a fluent API.
//
//	result, err := db.NewUpdate(&User{}).
//	    Set("name", "Jane").
//	    Set("email", "jane@example.com").
//	    Where("id = ?", 1).
//	    Exec(ctx)
type Update struct {
	exec    Executor
	model   any
	sets    []clause.Set
	wheres  []clause.Where
	columns []string
}

// NewUpdate creates a new UpdateBuilder.
func NewUpdate(exec Executor, model any) *Update {
	return &Update{exec: exec, model: model}
}

// Set adds a column = value pair to update.
func (b *Update) Set(column string, value any) *Update {
	b.sets = append(b.sets, clause.Set{Column: column, Value: value})
	return b
}

// Column specifies which columns to update from the model struct.
// If not called and no Set calls are made, all non-zero fields are updated.
//
//	db.NewUpdate(&user).Column("name", "email").WherePK().Exec(ctx)
func (b *Update) Column(columns ...string) *Update {
	b.columns = append(b.columns, columns...)
	return b
}

// Where adds a WHERE condition.
// Multiple calls are joined with AND.
func (b *Update) Where(condition string, args ...any) *Update {
	b.wheres = append(b.wheres, clause.Where{Condition: condition, Args: args})
	return b
}

// WherePK adds a WHERE condition using the model's primary key.
func (b *Update) WherePK() *Update {
	b.wheres = append(b.wheres, clause.Where{Condition: "_pk_", Args: nil})
	return b
}

// Exec executes the UPDATE query.
func (b *Update) Exec(ctx context.Context) (Result, error) {
	// TODO: Build SQL from sets/wheres and delegate to executor.
	_ = ctx
	return Result{}, nil
}
