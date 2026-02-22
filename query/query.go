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

	// Security warnings (for raw queries) - stored as interface{} to avoid import cycle
	Warnings []interface{}

	// Advanced SQL features
	CTEs         []*CTE        // Common Table Expressions (WITH clauses)
	Subqueries   []*Subquery   // Subqueries in various clauses
	WindowFuncs  []*WindowFunc // Window functions
	Having       []*Filter     // HAVING clause filters (post-aggregation)
}

// AddWarning adds a warning to the query
func (q *Query) AddWarning(warning interface{}) {
	q.Warnings = append(q.Warnings, warning)
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

	if q.Warnings != nil {
		newQ.Warnings = make([]interface{}, len(q.Warnings))
		copy(newQ.Warnings, q.Warnings)
	}

	if q.CTEs != nil {
		newQ.CTEs = make([]*CTE, len(q.CTEs))
		for i, cte := range q.CTEs {
			newQ.CTEs[i] = cte.Clone()
		}
	}

	if q.Subqueries != nil {
		newQ.Subqueries = make([]*Subquery, len(q.Subqueries))
		for i, sq := range q.Subqueries {
			newQ.Subqueries[i] = sq.Clone()
		}
	}

	if q.WindowFuncs != nil {
		newQ.WindowFuncs = make([]*WindowFunc, len(q.WindowFuncs))
		for i, wf := range q.WindowFuncs {
			newQ.WindowFuncs[i] = wf.Clone()
		}
	}

	if q.Having != nil {
		newQ.Having = make([]*Filter, len(q.Having))
		for i, f := range q.Having {
			newQ.Having[i] = f.Clone()
		}
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

// CTE represents a Common Table Expression (WITH clause)
type CTE struct {
	Name     string   // CTE name
	Alias    string   // Optional alias for the CTE
	Query    *Query   // The subquery for the CTE
	Columns  []string // Optional column names
	Recursive bool    // Whether this is a recursive CTE
}

// Clone creates a copy of the CTE
func (cte *CTE) Clone() *CTE {
	if cte == nil {
		return nil
	}
	columns := make([]string, len(cte.Columns))
	copy(columns, cte.Columns)
	return &CTE{
		Name:      cte.Name,
		Alias:     cte.Alias,
		Query:     cte.Query.Clone(),
		Columns:   columns,
		Recursive: cte.Recursive,
	}
}

// SubqueryType represents where a subquery is used
type SubqueryType string

const (
	SubqueryWhere  SubqueryType = "where"  // Subquery in WHERE clause
	SubqueryFrom   SubqueryType = "from"   // Subquery in FROM clause
	SubquerySelect SubqueryType = "select" // Subquery in SELECT clause
	SubqueryJoin   SubqueryType = "join"   // Subquery in JOIN clause
)

// Subquery represents a subquery
type Subquery struct {
	Query    *Query       // The subquery
	Type     SubqueryType // Where the subquery is used
	Alias    string       // Alias for the subquery (required for FROM/JOIN)
	Operator string       // Operator for WHERE subqueries (e.g., "IN", "EXISTS", "=")
	Field    string       // Field for comparison (for WHERE subqueries)
}

// Clone creates a copy of the subquery
func (s *Subquery) Clone() *Subquery {
	if s == nil {
		return nil
	}
	return &Subquery{
		Query:    s.Query.Clone(),
		Type:     s.Type,
		Alias:    s.Alias,
		Operator: s.Operator,
		Field:    s.Field,
	}
}

// WindowFunc represents a window function
type WindowFunc struct {
	Func       string   // Window function name (e.g., "ROW_NUMBER", "RANK", "SUM")
	Alias      string   // Alias for the window function result
	Expression string   // Expression (e.g., "salary" for SUM(salary))
	Partition  []string // PARTITION BY columns
	OrderBy    []*Order // ORDER BY clause
	Frame      *Frame   // Frame clause (ROWS BETWEEN...)
}

// Clone creates a copy of the window function
func (w *WindowFunc) Clone() *WindowFunc {
	if w == nil {
		return nil
	}
	partition := make([]string, len(w.Partition))
	copy(partition, w.Partition)

	orderBy := make([]*Order, len(w.OrderBy))
	copy(orderBy, w.OrderBy)

	return &WindowFunc{
		Func:       w.Func,
		Alias:      w.Alias,
		Expression: w.Expression,
		Partition:  partition,
		OrderBy:    orderBy,
		Frame:      w.Frame.Clone(),
	}
}

// Frame represents a window frame clause
type Frame struct {
	Mode      string // "ROWS", "RANGE", or "GROUPS"
	Start     string // Frame start (e.g., "UNBOUNDED PRECEDING", "CURRENT ROW", "2 PRECEDING")
	End       string // Frame end (e.g., "UNBOUNDED FOLLOWING", "CURRENT ROW", "2 FOLLOWING")
	Exclude   string // Optional: "CURRENT ROW", "GROUP", "TIES", "NO OTHERS"
}

// Clone creates a copy of the frame
func (f *Frame) Clone() *Frame {
	if f == nil {
		return nil
	}
	return &Frame{
		Mode:    f.Mode,
		Start:   f.Start,
		End:     f.End,
		Exclude: f.Exclude,
	}
}
