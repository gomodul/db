package dialect

import (
	"context"

	"github.com/gomodul/db/query"
)

// Transaction interface for backends that support it
type Transaction interface {
	// Commit commits the transaction
	Commit() error

	// Rollback rolls back the transaction
	Rollback() error

	// Query executes a query within the transaction
	Query(ctx context.Context, q *query.Query) (*Result, error)

	// Exec executes a command without returning rows (legacy, for compatibility)
	Exec(ctx context.Context, query string, args ...interface{}) (*Result, error)
}

// BaseTransaction provides a base implementation for transactions
type BaseTransaction struct {
	driver    Driver
	txID      string
	completed bool
}

// NewBaseTransaction creates a new base transaction
func NewBaseTransaction(driver Driver, txID string) *BaseTransaction {
	return &BaseTransaction{
		driver: driver,
		txID:   txID,
	}
}

// Query executes a query within the transaction
func (t *BaseTransaction) Query(ctx context.Context, q *query.Query) (*Result, error) {
	if t.completed {
		return nil, ErrTxDone
	}
	q = q.WithTransaction(t.txID)
	return t.driver.Execute(ctx, q)
}

// Exec executes a command within the transaction (legacy)
func (t *BaseTransaction) Exec(ctx context.Context, rawSQL string, args ...interface{}) (*Result, error) {
	if t.completed {
		return nil, ErrTxDone
	}
	// Create a raw query
	q := &query.Query{
		Raw:    rawSQL,
		RawArgs: args,
		IsRaw:  true,
		TxID:   t.txID,
	}
	return t.driver.Execute(ctx, q)
}

// MarkCompleted marks the transaction as completed
func (t *BaseTransaction) MarkCompleted() {
	t.completed = true
}

// IsCompleted returns true if the transaction is completed
func (t *BaseTransaction) IsCompleted() bool {
	return t.completed
}
