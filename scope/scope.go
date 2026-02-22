package scope

import (
	"time"

	"github.com/gomodul/db/builder"
	"github.com/gomodul/db/query"
)

// Scope is a function that modifies a QueryBuilder
type Scope func(*builder.QueryBuilder) *builder.QueryBuilder

// Registry manages named scopes
type Registry struct {
	scopes map[string]Scope
}

// NewRegistry creates a new scope registry
func NewRegistry() *Registry {
	return &Registry{
		scopes: make(map[string]Scope),
	}
}

// Register registers a named scope
func (r *Registry) Register(name string, scope Scope) {
	r.scopes[name] = scope
}

// Get retrieves a scope by name
func (r *Registry) Get(name string) (Scope, bool) {
	scope, ok := r.scopes[name]
	return scope, ok
}

// Unregister removes a scope
func (r *Registry) Unregister(name string) {
	delete(r.scopes, name)
}

// List returns all registered scope names
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.scopes))
	for name := range r.scopes {
		names = append(names, name)
	}
	return names
}

// Apply applies multiple scopes to a QueryBuilder
func Apply(qb *builder.QueryBuilder, scopes ...Scope) *builder.QueryBuilder {
	for _, scope := range scopes {
		if scope != nil {
			qb = scope(qb)
		}
	}
	return qb
}

// ApplyNamed applies named scopes from a registry
func ApplyNamed(qb *builder.QueryBuilder, registry *Registry, names ...string) *builder.QueryBuilder {
	for _, name := range names {
		if scope, ok := registry.Get(name); ok {
			qb = scope(qb)
		}
	}
	return qb
}

// ============ Common Predefined Scopes ============

// Active filters for records where active = true
func Active() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    "active",
			Operator: query.OpEqual,
			Value:    true,
		})
	}
}

// Inactive filters for records where active = false
func Inactive() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    "active",
			Operator: query.OpEqual,
			Value:    false,
		})
	}
}

// Published filters for published records
func Published() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    "status",
			Operator: query.OpEqual,
			Value:    "published",
		})
	}
}

// Draft filters for draft records
func Draft() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    "status",
			Operator: query.OpEqual,
			Value:    "draft",
		})
	}
}

// Recent filters for records created within the last duration
func Recent(duration time.Duration) Scope {
	cutoff := time.Now().Add(-duration)
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    "created_at",
			Operator: query.OpGreaterOrEqual,
			Value:    cutoff,
		})
	}
}

// Latest orders by created_at descending
func Latest() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Order("created_at DESC")
	}
}

// Oldest orders by created_at ascending
func Oldest() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Order("created_at ASC")
	}
}

// Limit sets a limit on the query
func Limit(limit int) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Limit(limit)
	}
}

// Offset sets an offset on the query
func Offset(offset int) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Offset(offset)
	}
}

// Paginate combines limit and offset for pagination
func Paginate(page, perPage int) Scope {
	offset := (page - 1) * perPage
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 10
	}
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Limit(perPage).Offset(offset)
	}
}

// Where creates a scope with a custom filter
func Where(field string, operator query.FilterOperator, value interface{}) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    field,
			Operator: operator,
			Value:    value,
		})
	}
}

// WhereEq creates a scope with an equality filter
func WhereEq(field string, value interface{}) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    field,
			Operator: query.OpEqual,
			Value:    value,
		})
	}
}

// WhereIn creates a scope with an IN filter
func WhereIn(field string, values ...interface{}) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.WhereIn(field, values...)
	}
}

// Order creates a scope with custom ordering
func Order(orderBy string) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Order(orderBy)
	}
}

// ============ Soft Delete Scopes ============

// NotDeleted excludes soft-deleted records
func NotDeleted() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.WhereNull("deleted_at")
	}
}

// Deleted only includes soft-deleted records
func Deleted() Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.WhereNotNull("deleted_at")
	}
}

// ============ Date Range Scopes ============

// DateRange filters for records within a date range
func DateRange(field string, start, end time.Time) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    field,
			Operator: query.OpGreaterOrEqual,
			Value:    start,
		}).Where(&query.Filter{
			Field:    field,
			Operator: query.OpLessOrEqual,
			Value:    end,
		})
	}
}

// Today filters for records created today
func Today(field string) Scope {
	start := time.Now().Truncate(24 * time.Hour)
	end := start.Add(24 * time.Hour)
	return DateRange(field, start, end)
}

// PastDays filters for records created within the past N days
func PastDays(field string, days int) Scope {
	cutoff := time.Now().AddDate(0, 0, -days)
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		return qb.Where(&query.Filter{
			Field:    field,
			Operator: query.OpGreaterOrEqual,
			Value:    cutoff,
		})
	}
}

// ============ Composition Helpers ============

// Combine creates a new scope from multiple scopes
func Combine(scopes ...Scope) Scope {
	return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
		for _, scope := range scopes {
			if scope != nil {
				qb = scope(qb)
			}
		}
		return qb
	}
}

// Conditional creates a scope that applies conditionally
func Conditional(condition bool, scope Scope) Scope {
	if !condition || scope == nil {
		return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
			return qb
		}
	}
	return scope
}

// Unless creates a scope that applies unless the condition is true
func Unless(condition bool, scope Scope) Scope {
	if condition || scope == nil {
		return func(qb *builder.QueryBuilder) *builder.QueryBuilder {
			return qb
		}
	}
	return scope
}
