package builder

import (
	"context"

	"github.com/gomodul/db/clause"
)

// Select builds a SELECT query using a fluent API.
//
//	var users []User
//	err := db.NewSelect(&users).
//	    Column("id", "name", "email").
//	    Where("age > ?", 18).
//	    OrderBy("name ASC").
//	    Limit(10).
//	    Exec(ctx)
type Select struct {
	exec    Executor
	dest    any
	columns []string
	wheres  []clause.Where
	orders  []string
	limit   int
	offset  int
}

// NewSelect creates a new SelectBuilder.
func NewSelect(exec Executor, dest any) *Select {
	return &Select{exec: exec, dest: dest}
}

// Column specifies which columns to select. If not called, all columns are selected.
func (b *Select) Column(columns ...string) *Select {
	b.columns = append(b.columns, columns...)
	return b
}

// Where adds a WHERE condition.
// Multiple calls are joined with AND.
//
//	b.Where("age > ?", 18).Where("name LIKE ?", "%john%")
func (b *Select) Where(condition string, args ...any) *Select {
	b.wheres = append(b.wheres, clause.Where{Condition: condition, Args: args})
	return b
}

// OrderBy adds an ORDER BY clause.
//
//	b.OrderBy("name ASC").OrderBy("created_at DESC")
func (b *Select) OrderBy(order string) *Select {
	b.orders = append(b.orders, order)
	return b
}

// Limit sets the maximum number of rows to return.
func (b *Select) Limit(n int) *Select {
	b.limit = n
	return b
}

// Offset sets the number of rows to skip before returning.
func (b *Select) Offset(n int) *Select {
	b.offset = n
	return b
}

// Exec executes the SELECT query and scans the results into dest.
func (b *Select) Exec(ctx context.Context) error {
	// TODO: Build SQL from clauses and delegate to executor.
	_ = ctx
	return nil
}
