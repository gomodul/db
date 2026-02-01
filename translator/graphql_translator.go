package translator

import (
	"fmt"
	"strings"

	"github.com/gomodul/db/query"
)

// GraphQLTranslator translates universal queries to GraphQL queries
type GraphQLTranslator struct {
	*BaseTranslator
	schema string // Optional schema for validation
}

// NewGraphQLTranslator creates a new GraphQL translator
func NewGraphQLTranslator() *GraphQLTranslator {
	return &GraphQLTranslator{
		BaseTranslator: NewBaseTranslator("graphql"),
	}
}

// GraphQLRequest represents a translated GraphQL request
type GraphQLRequest struct {
	Query         string
	OperationName string
	Variables     map[string]interface{}
}

// Translate converts universal query to GraphQL query
func (t *GraphQLTranslator) Translate(q *query.Query) (BackendQuery, error) {
	if err := t.Validate(q); err != nil {
		return nil, err
	}

	var graphql strings.Builder
	var variables = make(map[string]interface{})

	collection := t.getCollectionName(q)

	switch q.Operation {
	case query.OpFind:
		t.buildFindQuery(&graphql, q, collection, variables)
	case query.OpCreate:
		t.buildCreateMutation(&graphql, q, collection, variables)
	case query.OpUpdate:
		t.buildUpdateMutation(&graphql, q, collection, variables)
	case query.OpDelete:
		t.buildDeleteMutation(&graphql, q, collection, variables)
	case query.OpAggregate:
		t.buildAggregateQuery(&graphql, q, collection, variables)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, q.Operation)
	}

	return &GraphQLRequest{
		Query:     graphql.String(),
		Variables: variables,
	}, nil
}

func (t *GraphQLTranslator) getCollectionName(q *query.Query) string {
	if q.Collection != "" {
		return q.Collection
	}
	// TODO: Extract from model using reflection
	return "items"
}

func (t *GraphQLTranslator) buildFindQuery(builder *strings.Builder, q *query.Query, collection string, vars map[string]interface{}) {
	builder.WriteString("query {")

	// Build selection set
	fields := t.buildSelectionSet(q.Selects)
	builder.WriteString(fields)
	builder.WriteString("}")

	// Add arguments if there are filters
	if len(q.Filters) > 0 || q.Limit != nil || q.Offset != nil {
		builder.WriteString("(")
		args := t.buildQueryArguments(q, vars)
		builder.WriteString(args)
		builder.WriteString(")")
	}
}

func (t *GraphQLTranslator) buildCreateMutation(builder *strings.Builder, q *query.Query, collection string, vars map[string]interface{}) {
	builder.WriteString("mutation {")
	builder.WriteString("create")
	builder.WriteString(t.getCollectionName(q))

	if q.Document != nil {
		// Build input type
		builder.WriteString("(input: $input)")
		vars["input"] = q.Document
	}

	fields := t.buildSelectionSet(q.Selects)
	builder.WriteString(fields)
	builder.WriteString("}")
}

func (t *GraphQLTranslator) buildUpdateMutation(builder *strings.Builder, q *query.Query, collection string, vars map[string]interface{}) {
	builder.WriteString("mutation {")
	builder.WriteString("update")
	builder.WriteString(t.getCollectionName(q))

	args := t.buildQueryArguments(q, vars)
	if len(q.Updates) > 0 {
		if args != "" {
			args += ", "
		}
		args += "data: $data"
		vars["data"] = q.Updates
	}

	if args != "" {
		builder.WriteString("(")
		builder.WriteString(args)
		builder.WriteString(")")
	}

	fields := t.buildSelectionSet(q.Selects)
	builder.WriteString(fields)
	builder.WriteString("}")
}

func (t *GraphQLTranslator) buildDeleteMutation(builder *strings.Builder, q *query.Query, collection string, vars map[string]interface{}) {
	builder.WriteString("mutation {")
	builder.WriteString("delete")
	builder.WriteString(t.getCollectionName(q))

	args := t.buildQueryArguments(q, vars)
	if args != "" {
		builder.WriteString("(")
		builder.WriteString(args)
		builder.WriteString(")")
	}

	builder.WriteString("{ affectedRows }")
	builder.WriteString("}")
}

func (t *GraphQLTranslator) buildAggregateQuery(builder *strings.Builder, q *query.Query, collection string, vars map[string]interface{}) {
	builder.WriteString("query {")
	builder.WriteString("aggregate")
	builder.WriteString(t.getCollectionName(q))

	args := t.buildQueryArguments(q, vars)
	if len(q.Groups) > 0 {
		if args != "" {
			args += ", "
		}
		args += "groupBy: $groupBy"
		vars["groupBy"] = q.Groups
	}

	if args != "" {
		builder.WriteString("(")
		builder.WriteString(args)
		builder.WriteString(")")
	}

	builder.WriteString(" { ")
	for _, agg := range q.Aggregates {
		builder.WriteString(agg.Alias)
		builder.WriteString(" ")
	}
	builder.WriteString(" }")
	builder.WriteString("}")
}

func (t *GraphQLTranslator) buildSelectionSet(selects []string) string {
	if len(selects) == 0 {
		return "{ id }" // Default to id
	}
	return "{ " + strings.Join(selects, " ") + " }"
}

func (t *GraphQLTranslator) buildQueryArguments(q *query.Query, vars map[string]interface{}) string {
	var args []string

	// Build filter argument
	if len(q.Filters) > 0 {
		filter := t.buildGraphQLFilter(q.Filters)
		vars["where"] = filter
		args = append(args, "where: $where")
	}

	// Build pagination
	if q.Limit != nil {
		args = append(args, fmt.Sprintf("limit: %d", *q.Limit))
	}
	if q.Offset != nil {
		args = append(args, fmt.Sprintf("offset: %d", *q.Offset))
	}

	// Build sorting
	if len(q.Orders) > 0 {
		orderBy := make([]string, 0, len(q.Orders))
		for _, order := range q.Orders {
			if order.Direction == query.DirDesc {
				orderBy = append(orderBy, order.Field+"_DESC")
			} else {
				orderBy = append(orderBy, order.Field+"_ASC")
			}
		}
		args = append(args, "orderBy: ["+strings.Join(orderBy, ", ")+"]")
	}

	return strings.Join(args, ", ")
}

func (t *GraphQLTranslator) buildGraphQLFilter(filters []*query.Filter) map[string]interface{} {
	result := make(map[string]interface{})

	andConditions := make([]map[string]interface{}, 0)
	orConditions := make([]map[string]interface{}, 0)

	for _, filter := range filters {
		if len(filter.Nested) > 0 {
			nested := t.buildGraphQLFilter(filter.Nested)
			switch filter.Logic {
			case query.LogicAnd:
				andConditions = append(andConditions, nested)
			case query.LogicOr:
				orConditions = append(orConditions, nested)
			case query.LogicNot:
				result["NOT"] = nested
			}
		} else {
			condition := t.buildSingleGraphQLFilter(filter)
			switch filter.Logic {
			case query.LogicAnd, "":
				andConditions = append(andConditions, condition)
			case query.LogicOr:
				orConditions = append(orConditions, condition)
			case query.LogicNot:
				result["NOT"] = condition
			}
		}
	}

	if len(andConditions) > 0 {
		if len(andConditions) == 1 {
			for k, v := range andConditions[0] {
				result[k] = v
			}
		} else {
			result["AND"] = andConditions
		}
	}

	if len(orConditions) > 0 {
		if len(orConditions) == 1 {
			for k, v := range orConditions[0] {
				result[k] = v
			}
		} else {
			result["OR"] = orConditions
		}
	}

	return result
}

func (t *GraphQLTranslator) buildSingleGraphQLFilter(filter *query.Filter) map[string]interface{} {
	result := make(map[string]interface{})

	switch filter.Operator {
	case query.OpEqual:
		result[filter.Field] = filter.Value
	case query.OpNotEqual:
		result[filter.Field] = map[string]interface{}{
			"neq": filter.Value,
		}
	case query.OpGreaterThan:
		result[filter.Field] = map[string]interface{}{
			"gt": filter.Value,
		}
	case query.OpGreaterOrEqual:
		result[filter.Field] = map[string]interface{}{
			"gte": filter.Value,
		}
	case query.OpLessThan:
		result[filter.Field] = map[string]interface{}{
			"lt": filter.Value,
		}
	case query.OpLessOrEqual:
		result[filter.Field] = map[string]interface{}{
			"lte": filter.Value,
		}
	case query.OpIn:
		result[filter.Field] = map[string]interface{}{
			"in": filter.Values,
		}
	case query.OpNotIn:
		result[filter.Field] = map[string]interface{}{
			"nin": filter.Values,
		}
	case query.OpContains:
		result[filter.Field] = map[string]interface{}{
			"contains": filter.Value,
		}
	case query.OpStartsWith:
		result[filter.Field] = map[string]interface{}{
			"startsWith": filter.Value,
		}
	case query.OpEndsWith:
		result[filter.Field] = map[string]interface{}{
			"endsWith": filter.Value,
		}
	case query.OpNull:
		result[filter.Field] = nil
	case query.OpNotNull:
		result[filter.Field+"_not"] = nil
	case query.OpBetween:
		result[filter.Field] = map[string]interface{}{
			"between": []interface{}{filter.BetweenStart, filter.BetweenEnd},
		}
	}

	return result
}
