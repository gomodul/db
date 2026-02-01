package query

// Filter represents a universal filter condition
type Filter struct {
	Field    string
	Operator FilterOperator
	Value    interface{}
	Logic    LogicOperator // AND, OR, NOT
	Nested   []*Filter     // For complex nested conditions

	// For in/not_in operations
	Values []interface{}

	// For between operations
	BetweenStart interface{}
	BetweenEnd   interface{}
}

// FilterOperator represents comparison operators
type FilterOperator string

const (
	OpEqual          FilterOperator = "="
	OpNotEqual       FilterOperator = "!="
	OpGreaterThan    FilterOperator = ">"
	OpGreaterOrEqual FilterOperator = ">="
	OpLessThan       FilterOperator = "<"
	OpLessOrEqual    FilterOperator = "<="
	OpIn             FilterOperator = "in"
	OpNotIn          FilterOperator = "not_in"
	OpLike           FilterOperator = "like"
	OpNotLike        FilterOperator = "not_like"
	OpBetween        FilterOperator = "between"
	OpNull           FilterOperator = "is_null"
	OpNotNull        FilterOperator = "is_not_null"
	OpContains       FilterOperator = "contains"     // For arrays/strings
	OpStartsWith     FilterOperator = "starts_with"
	OpEndsWith       FilterOperator = "ends_with"
	OpWithin         FilterOperator = "within"       // Geospatial
	OpNear           FilterOperator = "near"         // Geospatial
	OpRegex          FilterOperator = "regex"
	OpExists         FilterOperator = "exists"       // Field existence
	OpType           FilterOperator = "type"         // Type check
	OpSize           FilterOperator = "size"         // Array size
	OpElemMatch      FilterOperator = "elem_match"  // Array element match
)

// LogicOperator represents logical operators
type LogicOperator string

const (
	LogicAnd LogicOperator = "AND"
	LogicOr  LogicOperator = "OR"
	LogicNot LogicOperator = "NOT"
)

// Clone creates a deep copy of the filter
func (f *Filter) Clone() *Filter {
	newF := &Filter{
		Field:    f.Field,
		Operator: f.Operator,
		Value:    f.Value,
		Logic:    f.Logic,
	}

	if f.Nested != nil {
		newF.Nested = make([]*Filter, len(f.Nested))
		for i, n := range f.Nested {
			newF.Nested[i] = n.Clone()
		}
	}

	if f.Values != nil {
		newF.Values = make([]interface{}, len(f.Values))
		copy(newF.Values, f.Values)
	}

	return newF
}

// And creates a new filter with AND logic
func (f *Filter) And(field string, op FilterOperator, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: op,
		Value:    value,
		Logic:    LogicAnd,
		Nested:   []*Filter{f},
	}
}

// Or creates a new filter with OR logic
func (f *Filter) Or(field string, op FilterOperator, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: op,
		Value:    value,
		Logic:    LogicOr,
		Nested:   []*Filter{f},
	}
}

// Not creates a new filter with NOT logic
func (f *Filter) Not() *Filter {
	return &Filter{
		Logic:  LogicNot,
		Nested: []*Filter{f},
	}
}

// Helper functions for creating filters

// Eq creates an equality filter
func Eq(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpEqual,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// Neq creates a not-equal filter
func Neq(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpNotEqual,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// Gt creates a greater-than filter
func Gt(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpGreaterThan,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// Gte creates a greater-than-or-equal filter
func Gte(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpGreaterOrEqual,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// Lt creates a less-than filter
func Lt(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpLessThan,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// Lte creates a less-than-or-equal filter
func Lte(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpLessOrEqual,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// In creates an IN filter
func In(field string, values ...interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpIn,
		Values:   values,
		Logic:    LogicAnd,
	}
}

// NotIn creates a NOT IN filter
func NotIn(field string, values ...interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpNotIn,
		Values:   values,
		Logic:    LogicAnd,
	}
}

// Like creates a LIKE filter
func Like(field string, pattern interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpLike,
		Value:    pattern,
		Logic:    LogicAnd,
	}
}

// Between creates a BETWEEN filter
func Between(field string, start, end interface{}) *Filter {
	return &Filter{
		Field:        field,
		Operator:     OpBetween,
		BetweenStart: start,
		BetweenEnd:   end,
		Logic:        LogicAnd,
	}
}

// IsNull creates an IS NULL filter
func IsNull(field string) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpNull,
		Logic:    LogicAnd,
	}
}

// IsNotNull creates an IS NOT NULL filter
func IsNotNull(field string) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpNotNull,
		Logic:    LogicAnd,
	}
}

// And combines multiple filters with AND logic
func And(filters ...*Filter) *Filter {
	return &Filter{
		Logic:  LogicAnd,
		Nested: filters,
	}
}

// Or combines multiple filters with OR logic
func Or(filters ...*Filter) *Filter {
	return &Filter{
		Logic:  LogicOr,
		Nested: filters,
	}
}

// Not creates a NOT filter
func Not(filter *Filter) *Filter {
	return &Filter{
		Logic:  LogicNot,
		Nested: []*Filter{filter},
	}
}

// Contains creates a contains filter for strings/arrays
func Contains(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpContains,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// StartsWith creates a starts-with filter
func StartsWith(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpStartsWith,
		Value:    value,
		Logic:    LogicAnd,
	}
}

// EndsWith creates an ends-with filter
func EndsWith(field string, value interface{}) *Filter {
	return &Filter{
		Field:    field,
		Operator: OpEndsWith,
		Value:    value,
		Logic:    LogicAnd,
	}
}
