package translator

import (
	"fmt"
	"strings"

	"github.com/gomodul/db/query"
)

// SQLTranslator translates universal queries to SQL
type SQLTranslator struct {
	*BaseTranslator
	dialect SQLDialect
}

// SQLDialect represents SQL dialect-specific behavior
type SQLDialect interface {
	// Name returns the dialect name
	Name() string

	// BindVar returns the bind variable format (e.g., "?", "$1", "$2")
	BindVar(idx int) string

	// QuoteIdentifier quotes an identifier (table/column name)
	QuoteIdentifier(name string) string

	// Supports returns true if the dialect supports a feature
	Supports(feature SQLFeature) bool
}

// SQLFeature represents SQL dialect features
type SQLFeature string

const (
	FeatureWindowFunctions    SQLFeature = "window_functions"
	FeatureCTE                SQLFeature = "cte"
	FeatureFullOuterJoin      SQLFeature = "full_outer_join"
	FeatureReturningClause    SQLFeature = "returning"
	FeatureUpsert             SQLFeature = "upsert"
	FeatureIgnoreConflict     SQLFeature = "ignore_conflict"
	FeatureFilteredAggregates SQLFeature = "filtered_aggregates"
)

// NewSQLTranslator creates a new SQL translator
func NewSQLTranslator(dialect SQLDialect) *SQLTranslator {
	return &SQLTranslator{
		BaseTranslator: NewBaseTranslator("sql"),
		dialect:       dialect,
	}
}

// Translate converts universal query to SQL
func (t *SQLTranslator) Translate(q *query.Query) (BackendQuery, error) {
	if err := t.Validate(q); err != nil {
		return nil, err
	}

	var sqlBuilder strings.Builder
	var args []interface{}
	bindIdx := 1

	switch q.Operation {
	case query.OpFind:
		t.buildSelect(&sqlBuilder, q, &args, &bindIdx)
	case query.OpCreate:
		t.buildInsert(&sqlBuilder, q, &args, &bindIdx)
	case query.OpUpdate:
		t.buildUpdate(&sqlBuilder, q, &args, &bindIdx)
	case query.OpDelete:
		t.buildDelete(&sqlBuilder, q, &args, &bindIdx)
	case query.OpCount:
		t.buildCount(&sqlBuilder, q, &args, &bindIdx)
	case query.OpAggregate:
		t.buildAggregate(&sqlBuilder, q, &args, &bindIdx)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, q.Operation)
	}

	return &SQLQuery{
		SQL:  sqlBuilder.String(),
		Args: args,
	}, nil
}

// SQLQuery represents a translated SQL query
type SQLQuery struct {
	SQL  string
	Args []interface{}
}

func (t *SQLTranslator) buildSelect(builder *strings.Builder, q *query.Query, args *[]interface{}, bindIdx *int) {
	builder.WriteString("SELECT ")

	// Build SELECT clause
	if len(q.Selects) > 0 {
		for i, col := range q.Selects {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(t.dialect.QuoteIdentifier(col))
		}
	} else {
		builder.WriteString("*")
	}

	builder.WriteString(" FROM ")
	builder.WriteString(t.dialect.QuoteIdentifier(q.Collection))

	// Build JOINs
	for _, join := range q.Joins {
		t.buildJoin(builder, join, args, bindIdx)
	}

	// Build WHERE clause
	if len(q.Filters) > 0 {
		t.buildWhere(builder, q.Filters, args, bindIdx)
	}

	// Build GROUP BY
	if len(q.Groups) > 0 {
		builder.WriteString(" GROUP BY ")
		for i, group := range q.Groups {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(t.dialect.QuoteIdentifier(group))
		}
	}

	// Build ORDER BY
	if len(q.Orders) > 0 {
		builder.WriteString(" ORDER BY ")
		for i, order := range q.Orders {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(t.dialect.QuoteIdentifier(order.Field))
			builder.WriteString(" ")
			builder.WriteString(string(order.Direction))
		}
	}

	// Build LIMIT and OFFSET
	if q.Limit != nil {
		builder.WriteString(fmt.Sprintf(" LIMIT %d", *q.Limit))
	}
	if q.Offset != nil {
		builder.WriteString(fmt.Sprintf(" OFFSET %d", *q.Offset))
	}
}

func (t *SQLTranslator) buildInsert(builder *strings.Builder, q *query.Query, args *[]interface{}, bindIdx *int) {
	builder.WriteString("INSERT INTO ")
	builder.WriteString(t.dialect.QuoteIdentifier(q.Collection))
	builder.WriteString(" (")

	// TODO: Extract fields from model/document
	builder.WriteString(") VALUES (")

	// TODO: Add bind variables
	builder.WriteString(")")

	if t.dialect.Supports(FeatureReturningClause) && q.Operation == query.OpCreate {
		builder.WriteString(" RETURNING *")
	}
}

func (t *SQLTranslator) buildUpdate(builder *strings.Builder, q *query.Query, args *[]interface{}, bindIdx *int) {
	builder.WriteString("UPDATE ")
	builder.WriteString(t.dialect.QuoteIdentifier(q.Collection))
	builder.WriteString(" SET ")

	// TODO: Build SET clause from updates

	if len(q.Filters) > 0 {
		t.buildWhere(builder, q.Filters, args, bindIdx)
	}
}

func (t *SQLTranslator) buildDelete(builder *strings.Builder, q *query.Query, args *[]interface{}, bindIdx *int) {
	builder.WriteString("DELETE FROM ")
	builder.WriteString(t.dialect.QuoteIdentifier(q.Collection))

	if len(q.Filters) > 0 {
		t.buildWhere(builder, q.Filters, args, bindIdx)
	}
}

func (t *SQLTranslator) buildCount(builder *strings.Builder, q *query.Query, args *[]interface{}, bindIdx *int) {
	builder.WriteString("SELECT COUNT(*) FROM ")
	builder.WriteString(t.dialect.QuoteIdentifier(q.Collection))

	if len(q.Filters) > 0 {
		t.buildWhere(builder, q.Filters, args, bindIdx)
	}
}

func (t *SQLTranslator) buildAggregate(builder *strings.Builder, q *query.Query, args *[]interface{}, bindIdx *int) {
	builder.WriteString("SELECT ")

	for i, agg := range q.Aggregates {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(fmt.Sprintf("%s(%s)", agg.Operator, t.dialect.QuoteIdentifier(agg.Field)))
		if agg.Alias != "" {
			builder.WriteString(" AS ")
			builder.WriteString(t.dialect.QuoteIdentifier(agg.Alias))
		}
	}

	builder.WriteString(" FROM ")
	builder.WriteString(t.dialect.QuoteIdentifier(q.Collection))

	if len(q.Filters) > 0 {
		t.buildWhere(builder, q.Filters, args, bindIdx)
	}

	if len(q.Groups) > 0 {
		builder.WriteString(" GROUP BY ")
		for i, group := range q.Groups {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(t.dialect.QuoteIdentifier(group))
		}
	}
}

func (t *SQLTranslator) buildWhere(builder *strings.Builder, filters []*query.Filter, args *[]interface{}, bindIdx *int) {
	builder.WriteString(" WHERE ")
	t.buildFilterExpression(builder, filters, args, bindIdx)
}

func (t *SQLTranslator) buildFilterExpression(builder *strings.Builder, filters []*query.Filter, args *[]interface{}, bindIdx *int) {
	for i, filter := range filters {
		if i > 0 {
			if filter.Logic != "" {
				builder.WriteString(" ")
				builder.WriteString(string(filter.Logic))
				builder.WriteString(" ")
			}
		}

		if len(filter.Nested) > 0 {
			builder.WriteString("(")
			t.buildFilterExpression(builder, filter.Nested, args, bindIdx)
			builder.WriteString(")")
		} else {
			t.buildSingleFilter(builder, filter, args, bindIdx)
		}
	}
}

func (t *SQLTranslator) buildSingleFilter(builder *strings.Builder, filter *query.Filter, args *[]interface{}, bindIdx *int) {
	builder.WriteString(t.dialect.QuoteIdentifier(filter.Field))
	builder.WriteString(" ")
	builder.WriteString(string(filter.Operator))
	builder.WriteString(" ")

	switch filter.Operator {
	case query.OpIn, query.OpNotIn:
		builder.WriteString("(")
		for i, v := range filter.Values {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(t.dialect.BindVar(*bindIdx))
			*args = append(*args, v)
			*bindIdx++
		}
		builder.WriteString(")")
	case query.OpBetween:
		builder.WriteString(t.dialect.BindVar(*bindIdx))
		*args = append(*args, filter.BetweenStart)
		*bindIdx++
		builder.WriteString(" AND ")
		builder.WriteString(t.dialect.BindVar(*bindIdx))
		*args = append(*args, filter.BetweenEnd)
		*bindIdx++
	case query.OpNull, query.OpNotNull:
		// No bind variable needed
	default:
		builder.WriteString(t.dialect.BindVar(*bindIdx))
		*args = append(*args, filter.Value)
		*bindIdx++
	}
}

func (t *SQLTranslator) buildJoin(builder *strings.Builder, join *query.Join, args *[]interface{}, bindIdx *int) {
	builder.WriteString(" ")
	builder.WriteString(string(join.Type))
	builder.WriteString(" JOIN ")
	builder.WriteString(t.dialect.QuoteIdentifier(join.Collection))
	if join.Alias != "" {
		builder.WriteString(" AS ")
		builder.WriteString(t.dialect.QuoteIdentifier(join.Alias))
	}

	if len(join.Conditions) > 0 {
		builder.WriteString(" ON ")
		t.buildFilterExpression(builder, join.Conditions, args, bindIdx)
	}
}
