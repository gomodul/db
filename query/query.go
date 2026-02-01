package query

import (
	"context"
	"time"
)

// OperationType represents the type of CRUD operation
type OperationType string

const (
	OpFind      OperationType = "find"
	OpCreate    OperationType = "create"
	OpUpdate    OperationType = "update"
	OpDelete    OperationType = "delete"
	OpCount     OperationType = "count"
	OpCheckExists OperationType = "exists" // Renamed to avoid conflict with FilterOperator
	OpAggregate OperationType = "aggregate"
	OpUpsert    OperationType = "upsert"
)

// Query represents a universal, backend-agnostic query
type Query struct {
	// Core operation
	Operation OperationType

	// Target
	Collection string   // Table, collection, index, endpoint, etc.
	Model      interface{} // Go model for struct mapping

	// Data operations
	Document   interface{} // For create/update operations
	Documents  []interface{} // For batch operations

	// Filtering (universal where clause)
	Filters []*Filter

	// Sorting
	Orders []*Order

	// Pagination
	Limit  *int
	Offset *int
	Cursor *Cursor

	// Projection
	Selects []string // Fields to select

	// Relationships (for joins/preloads)
	Joins    []*Join
	Preloads []string

	// Aggregation
	Groups     []string
	Aggregates []*Aggregate

	// Metadata
	Hints   map[string]interface{}
	Context context.Context

	// Transaction info
	TxID string

	// For update operations
	Updates map[string]interface{}

	// Raw query (for when native query is needed)
	Raw       string
	RawArgs   []interface{}
	IsRaw     bool
}

// Clone creates a deep copy of the query
func (q *Query) Clone() *Query {
	newQ := &Query{
		Operation:  q.Operation,
		Collection: q.Collection,
		Model:      q.Model,
		Document:   q.Document,
		Limit:      q.Limit,
		Offset:     q.Offset,
		Context:    q.Context,
		TxID:       q.TxID,
		Raw:        q.Raw,
		IsRaw:      q.IsRaw,
	}

	if q.Documents != nil {
		newQ.Documents = make([]interface{}, len(q.Documents))
		copy(newQ.Documents, q.Documents)
	}

	if q.Filters != nil {
		newQ.Filters = make([]*Filter, len(q.Filters))
		for i, f := range q.Filters {
			newQ.Filters[i] = f.Clone()
		}
	}

	if q.Orders != nil {
		newQ.Orders = make([]*Order, len(q.Orders))
		copy(newQ.Orders, q.Orders)
	}

	if q.Selects != nil {
		newQ.Selects = make([]string, len(q.Selects))
		copy(newQ.Selects, q.Selects)
	}

	if q.Joins != nil {
		newQ.Joins = make([]*Join, len(q.Joins))
		for i, j := range q.Joins {
			newQ.Joins[i] = j.Clone()
		}
	}

	if q.Preloads != nil {
		newQ.Preloads = make([]string, len(q.Preloads))
		copy(newQ.Preloads, q.Preloads)
	}

	if q.Groups != nil {
		newQ.Groups = make([]string, len(q.Groups))
		copy(newQ.Groups, q.Groups)
	}

	if q.Aggregates != nil {
		newQ.Aggregates = make([]*Aggregate, len(q.Aggregates))
		copy(newQ.Aggregates, q.Aggregates)
	}

	if q.Hints != nil {
		newQ.Hints = make(map[string]interface{})
		for k, v := range q.Hints {
			newQ.Hints[k] = v
		}
	}

	if q.Updates != nil {
		newQ.Updates = make(map[string]interface{})
		for k, v := range q.Updates {
			newQ.Updates[k] = v
		}
	}

	if q.RawArgs != nil {
		newQ.RawArgs = make([]interface{}, len(q.RawArgs))
		copy(newQ.RawArgs, q.RawArgs)
	}

	if q.Cursor != nil {
		newQ.Cursor = q.Cursor.Clone()
	}

	return newQ
}

// WithContext returns a new query with the given context
func (q *Query) WithContext(ctx context.Context) *Query {
	newQ := q.Clone()
	newQ.Context = ctx
	return newQ
}

// WithTransaction returns a new query with the given transaction ID
func (q *Query) WithTransaction(txID string) *Query {
	newQ := q.Clone()
	newQ.TxID = txID
	return newQ
}

// Cursor represents cursor-based pagination
type Cursor struct {
	Value interface{}
	Field string
	Time  time.Time // For timestamp-based cursors
}

// Clone creates a copy of the cursor
func (c *Cursor) Clone() *Cursor {
	if c == nil {
		return nil
	}
	return &Cursor{
		Value: c.Value,
		Field: c.Field,
		Time:  c.Time,
	}
}
