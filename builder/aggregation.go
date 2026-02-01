package builder

import (
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// ============ Aggregation Methods ============

// Count returns the count of records
func (b *QueryBuilder) Count() (int64, error) {
	b.q.Operation = query.OpCount
	result, err := b.execute()
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

// CountX is like Count but allows specifying field
func (b *QueryBuilder) CountX(field string) (int64, error) {
	b.q.Aggregates = []*query.Aggregate{
		query.Count(field, "count"),
	}
	b.q.Operation = query.OpAggregate

	result, err := b.execute()
	if err != nil {
		return 0, err
	}

	// Extract count from result
	if len(result.Data) > 0 {
		if m, ok := result.Data[0].(map[string]interface{}); ok {
			if count, ok := m["count"].(int64); ok {
				return count, nil
			}
		}
	}

	return 0, nil
}

// Sum calculates the sum of a field
func (b *QueryBuilder) Sum(field string) (int64, error) {
	return b.aggregateSum(field)
}

// SumFloat returns sum as float64
func (b *QueryBuilder) SumFloat(field string) (float64, error) {
	return b.aggregateSumFloat(field)
}

// Avg calculates the average of a field
func (b *QueryBuilder) Avg(field string) (float64, error) {
	return b.aggregateAvg(field)
}

// Min returns the minimum value of a field
func (b *QueryBuilder) Min(field string) (interface{}, error) {
	return b.aggregateMin(field)
}

// Max returns the maximum value of a field
func (b *QueryBuilder) Max(field string) (interface{}, error) {
	return b.aggregateMax(field)
}

// Pluck queries a single column and returns the values
func (b *QueryBuilder) Pluck(column string, dest interface{}) error {
	b.q.Selects = []string{column}
	b.q.Operation = query.OpFind

	result, err := b.execute()
	if err != nil {
		return err
	}

	return b.scanResult(dest, result.Data)
}

// PluckSlice queries a column and returns as []interface{}
func (b *QueryBuilder) PluckSlice(column string) ([]interface{}, error) {
	var results []interface{}
	err := b.Pluck(column, &results)
	return results, err
}

// PluckInt64 queries a column and returns as []int64
func (b *QueryBuilder) PluckInt64(column string) ([]int64, error) {
	var results []int64
	err := b.Pluck(column, &results)
	return results, err
}

// PluckStrings queries a column and returns as []string
func (b *QueryBuilder) PluckStrings(column string) ([]string, error) {
	var results []string
	err := b.Pluck(column, &results)
	return results, err}

// ============ Group By Methods ============

// Group specifies fields to group by
func (b *QueryBuilder) Group(fields ...string) *QueryBuilder {
	b.q.Groups = fields
	return b
}

// GroupBy is an alias for Group
func (b *QueryBuilder) GroupBy(fields ...string) *QueryBuilder {
	return b.Group(fields...)
}

// Having adds a HAVING clause for aggregated queries
func (b *QueryBuilder) Having(filter interface{}, args ...interface{}) *QueryBuilder {
	_ = b.parseFilter(filter, args...)
	// Store separately (implementation would add Having support to query.Query)
	if b.q.Hints == nil {
		b.q.Hints = make(map[string]interface{})
	}
	b.q.Hints["having"] = filter
	return b
}

// ============ Distinct Methods ============

// Distinct adds DISTINCT to the query
func (b *QueryBuilder) Distinct() *QueryBuilder {
	if b.q.Hints == nil {
		b.q.Hints = make(map[string]interface{})
	}
	b.q.Hints["distinct"] = true
	return b
}

// DistinctOn specifies DISTINCT ON columns
func (b *QueryBuilder) DistinctOn(columns ...string) *QueryBuilder {
	if b.q.Hints == nil {
		b.q.Hints = make(map[string]interface{})
	}
	b.q.Hints["distinct_on"] = columns
	b.q.Selects = append(b.q.Selects, columns...)
	return b
}

// ============ Internal Aggregation Methods ============

func (b *QueryBuilder) aggregateSum(field string) (int64, error) {
	b.q.Aggregates = []*query.Aggregate{
		query.Sum(field, "sum"),
	}
	b.q.Operation = query.OpAggregate

	result, err := b.execute()
	if err != nil {
		return 0, err
	}

	return b.extractInt64Result(result, "sum")
}

func (b *QueryBuilder) aggregateSumFloat(field string) (float64, error) {
	b.q.Aggregates = []*query.Aggregate{
		query.Sum(field, "sum"),
	}
	b.q.Operation = query.OpAggregate

	result, err := b.execute()
	if err != nil {
		return 0, err
	}

	return b.extractFloat64Result(result, "sum")
}

func (b *QueryBuilder) aggregateAvg(field string) (float64, error) {
	b.q.Aggregates = []*query.Aggregate{
		query.Avg(field, "avg"),
	}
	b.q.Operation = query.OpAggregate

	result, err := b.execute()
	if err != nil {
		return 0, err
	}

	return b.extractFloat64Result(result, "avg")
}

func (b *QueryBuilder) aggregateMin(field string) (interface{}, error) {
	b.q.Aggregates = []*query.Aggregate{
		query.Min(field, "min"),
	}
	b.q.Operation = query.OpAggregate

	result, err := b.execute()
	if err != nil {
		return nil, err
	}

	return b.extractInterfaceResult(result, "min")
}

func (b *QueryBuilder) aggregateMax(field string) (interface{}, error) {
	b.q.Aggregates = []*query.Aggregate{
		query.Max(field, "max"),
	}
	b.q.Operation = query.OpAggregate

	result, err := b.execute()
	if err != nil {
		return nil, err
	}

	return b.extractInterfaceResult(result, "max")
}

// Result extraction helpers

func (b *QueryBuilder) extractInt64Result(result *dialect.Result, key string) (int64, error) {
	if len(result.Data) == 0 {
		return 0, nil
	}

	if m, ok := result.Data[0].(map[string]interface{}); ok {
		if val, ok := m[key]; ok {
			switch v := val.(type) {
			case int64:
				return v, nil
			case int:
				return int64(v), nil
			case float64:
				return int64(v), nil
			}
		}
	}

	return 0, nil
}

func (b *QueryBuilder) extractFloat64Result(result *dialect.Result, key string) (float64, error) {
	if len(result.Data) == 0 {
		return 0, nil
	}

	if m, ok := result.Data[0].(map[string]interface{}); ok {
		if val, ok := m[key]; ok {
			switch v := val.(type) {
			case float64:
				return v, nil
			case float32:
				return float64(v), nil
			case int64:
				return float64(v), nil
			case int:
				return float64(v), nil
			}
		}
	}

	return 0, nil
}

func (b *QueryBuilder) extractInterfaceResult(result *dialect.Result, key string) (interface{}, error) {
	if len(result.Data) == 0 {
		return nil, nil
	}

	if m, ok := result.Data[0].(map[string]interface{}); ok {
		return m[key], nil
	}

	return nil, nil
}
