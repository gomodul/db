package builder

import (
	"github.com/gomodul/db/query"
)

// ============ Additional Filter Helper Functions ============

// WhereOp adds a filter with a specific operator
func (b *QueryBuilder) WhereOp(field string, operator query.FilterOperator, value interface{}) *QueryBuilder {
	return b.Where(&query.Filter{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
}

// Scopes adds scopes to the query
func (b *QueryBuilder) Scopes(funcs ...func(*QueryBuilder) *QueryBuilder) *QueryBuilder {
	for _, f := range funcs {
		b = f(b)
	}
	return b
}
