package query

// Aggregate represents an aggregation operation
type Aggregate struct {
	Operator AggOperator
	Field    string
	Alias    string
}

// AggOperator represents aggregation operators
type AggOperator string

const (
	AggOpCount AggOperator = "count"
	AggOpSum   AggOperator = "sum"
	AggOpAvg   AggOperator = "avg"
	AggOpMin   AggOperator = "min"
	AggOpMax   AggOperator = "max"
	AggOpFirst AggOperator = "first"
	AggOpLast  AggOperator = "last"
)

// Count creates a COUNT aggregation
func Count(field string, alias ...string) *Aggregate {
	a := &Aggregate{
		Operator: AggOpCount,
		Field:    field,
	}
	if len(alias) > 0 {
		a.Alias = alias[0]
	}
	return a
}

// Sum creates a SUM aggregation
func Sum(field string, alias ...string) *Aggregate {
	a := &Aggregate{
		Operator: AggOpSum,
		Field:    field,
	}
	if len(alias) > 0 {
		a.Alias = alias[0]
	}
	return a
}

// Avg creates an AVG aggregation
func Avg(field string, alias ...string) *Aggregate {
	a := &Aggregate{
		Operator: AggOpAvg,
		Field:    field,
	}
	if len(alias) > 0 {
		a.Alias = alias[0]
	}
	return a
}

// Min creates a MIN aggregation
func Min(field string, alias ...string) *Aggregate {
	a := &Aggregate{
		Operator: AggOpMin,
		Field:    field,
	}
	if len(alias) > 0 {
		a.Alias = alias[0]
	}
	return a
}

// Max creates a MAX aggregation
func Max(field string, alias ...string) *Aggregate {
	a := &Aggregate{
		Operator: AggOpMax,
		Field:    field,
	}
	if len(alias) > 0 {
		a.Alias = alias[0]
	}
	return a
}
