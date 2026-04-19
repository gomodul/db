package translator

import (
	"fmt"

	"github.com/gomodul/db/query"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MongoDBTranslator translates universal queries to MongoDB queries
type MongoDBTranslator struct {
	*BaseTranslator
}

// NewMongoDBTranslator creates a new MongoDB translator
func NewMongoDBTranslator() *MongoDBTranslator {
	return &MongoDBTranslator{
		BaseTranslator: NewBaseTranslator("mongodb"),
	}
}

// Translate converts universal query to MongoDB query
func (t *MongoDBTranslator) Translate(q *query.Query) (BackendQuery, error) {
	if err := t.Validate(q); err != nil {
		return nil, err
	}

	mongoQuery := &MongoDBQuery{}

	switch q.Operation {
	case query.OpFind:
		mongoQuery.Operation = "find"
		mongoQuery.Filter = t.translateFilters(q.Filters)
		mongoQuery.Projection = t.translateProjection(q.Selects)
		mongoQuery.Sort = t.translateSort(q.Orders)
		mongoQuery.Limit = q.Limit
		mongoQuery.Skip = q.Offset
		mongoQuery.Pipeline = t.buildPipeline(q)

	case query.OpCreate:
		mongoQuery.Operation = "insertOne"
		if q.Document != nil {
			mongoQuery.Document = q.Document
		}

	case query.OpUpdate:
		mongoQuery.Operation = "updateMany"
		mongoQuery.Filter = t.translateFilters(q.Filters)
		mongoQuery.Update = t.translateUpdate(q)

	case query.OpDelete:
		mongoQuery.Operation = "deleteMany"
		mongoQuery.Filter = t.translateFilters(q.Filters)

	case query.OpCount:
		mongoQuery.Operation = "count"
		mongoQuery.Filter = t.translateFilters(q.Filters)

	case query.OpAggregate:
		mongoQuery.Operation = "aggregate"
		mongoQuery.Pipeline = t.buildAggregatePipeline(q)

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, q.Operation)
	}

	return mongoQuery, nil
}

// MongoDBQuery represents a translated MongoDB query
type MongoDBQuery struct {
	Operation  string
	Filter     bson.M
	Update     bson.M
	Document   interface{}
	Documents  []interface{}
	Projection bson.M
	Sort       bson.D
	Limit      *int
	Skip       *int
	Pipeline   []primitive.M
	Options    map[string]interface{}
}

func (t *MongoDBTranslator) translateFilters(filters []*query.Filter) bson.M {
	if len(filters) == 0 {
		return nil
	}

	result := make(bson.M)
	andConditions := []interface{}{}
	orConditions := []interface{}{}

	for _, filter := range filters {
		if len(filter.Nested) > 0 {
			nested := t.translateFilters(filter.Nested)
			switch filter.Logic {
			case query.LogicAnd:
				andConditions = append(andConditions, nested)
			case query.LogicOr:
				orConditions = append(orConditions, nested)
			case query.LogicNot:
				result["$not"] = nested
			}
		} else {
			condition := t.translateSingleFilter(filter)
			switch filter.Logic {
			case query.LogicAnd, "":
				andConditions = append(andConditions, condition)
			case query.LogicOr:
				orConditions = append(orConditions, condition)
			case query.LogicNot:
				result["$not"] = condition
			}
		}
	}

	if len(andConditions) > 0 {
		if len(andConditions) == 1 {
			for k, v := range andConditions[0].(bson.M) {
				result[k] = v
			}
		} else {
			result["$and"] = andConditions
		}
	}

	if len(orConditions) > 0 {
		if len(orConditions) == 1 {
			result["$or"] = orConditions
		} else {
			result["$or"] = orConditions
		}
	}

	return result
}

func (t *MongoDBTranslator) translateSingleFilter(filter *query.Filter) bson.M {
	result := make(bson.M)

	switch filter.Operator {
	case query.OpEqual:
		result[filter.Field] = filter.Value
	case query.OpNotEqual:
		result[filter.Field] = bson.M{"$ne": filter.Value}
	case query.OpGreaterThan:
		result[filter.Field] = bson.M{"$gt": filter.Value}
	case query.OpGreaterOrEqual:
		result[filter.Field] = bson.M{"$gte": filter.Value}
	case query.OpLessThan:
		result[filter.Field] = bson.M{"$lt": filter.Value}
	case query.OpLessOrEqual:
		result[filter.Field] = bson.M{"$lte": filter.Value}
	case query.OpIn:
		result[filter.Field] = bson.M{"$in": filter.Values}
	case query.OpNotIn:
		result[filter.Field] = bson.M{"$nin": filter.Values}
	case query.OpContains:
		result[filter.Field] = bson.M{"$regex": fmt.Sprintf(".*%v.*", filter.Value), "$options": "i"}
	case query.OpStartsWith:
		result[filter.Field] = bson.M{"$regex": fmt.Sprintf("^%v", filter.Value), "$options": "i"}
	case query.OpEndsWith:
		result[filter.Field] = bson.M{"$regex": fmt.Sprintf("%v$", filter.Value), "$options": "i"}
	case query.OpNull:
		result[filter.Field] = bson.M{"$exists": false}
	case query.OpNotNull:
		result[filter.Field] = bson.M{"$exists": true}
	case query.OpRegex:
		result[filter.Field] = bson.M{"$regex": filter.Value}
	case query.OpBetween:
		result[filter.Field] = bson.M{
			"$gte": filter.BetweenStart,
			"$lte": filter.BetweenEnd,
		}
	}

	return result
}

func (t *MongoDBTranslator) translateProjection(selects []string) bson.M {
	if len(selects) == 0 {
		return nil
	}

	projection := make(bson.M)
	for _, col := range selects {
		projection[col] = 1
	}
	return projection
}

func (t *MongoDBTranslator) translateSort(orders []*query.Order) bson.D {
	if len(orders) == 0 {
		return nil
	}

	sort := make(bson.D, len(orders))
	for i, order := range orders {
		direction := 1
		if order.Direction == query.DirDesc {
			direction = -1
		}
		sort[i] = primitive.E{Key: order.Field, Value: direction}
	}
	return sort
}

func (t *MongoDBTranslator) translateUpdate(q *query.Query) bson.M {
	update := make(bson.M)

	setFields := make(bson.M)
	unsetFields := make(bson.M)

	if q.Document != nil {
		cols, vals := extractFields(q.Document)
		for i, col := range cols {
			setFields[col] = vals[i]
		}
	}

	if q.Updates != nil {
		for k, v := range q.Updates {
			if v == nil {
				unsetFields[k] = ""
			} else {
				setFields[k] = v
			}
		}
	}

	if len(setFields) > 0 {
		update["$set"] = setFields
	}
	if len(unsetFields) > 0 {
		update["$unset"] = unsetFields
	}

	return update
}

func (t *MongoDBTranslator) buildPipeline(q *query.Query) []primitive.M {
	var pipeline []primitive.M

	// Match stage
	if len(q.Filters) > 0 {
		filter := t.translateFilters(q.Filters)
		pipeline = append(pipeline, bson.M{"$match": filter})
	}

	// Lookup stage for joins
	for _, join := range q.Joins {
		lookup := bson.M{
			"$lookup": bson.M{
				"from":         join.Collection,
				"localField":   join.ForeignKeys[0],
				"foreignField": join.References[0],
				"as":           join.Alias,
			},
		}
		pipeline = append(pipeline, lookup)
	}

	// Group stage
	if len(q.Groups) > 0 {
		groupID := make(bson.M)
		for _, g := range q.Groups {
			groupID[g] = "$" + g
		}

		group := bson.M{"_id": groupID}

		// Add aggregations
		for _, agg := range q.Aggregates {
			field := "$" + agg.Field
			switch agg.Operator {
			case query.AggOpCount:
				group[agg.Alias] = bson.M{"$sum": 1}
			case query.AggOpSum:
				group[agg.Alias] = bson.M{"$sum": field}
			case query.AggOpAvg:
				group[agg.Alias] = bson.M{"$avg": field}
			case query.AggOpMin:
				group[agg.Alias] = bson.M{"$min": field}
			case query.AggOpMax:
				group[agg.Alias] = bson.M{"$max": field}
			}
		}

		pipeline = append(pipeline, bson.M{"$group": group})
	}

	// Sort stage
	if len(q.Orders) > 0 {
		sort := make(bson.D, 0, len(q.Orders))
		for _, order := range q.Orders {
			direction := 1
			if order.Direction == query.DirDesc {
				direction = -1
			}
			sort = append(sort, primitive.E{Key: order.Field, Value: direction})
		}
		pipeline = append(pipeline, bson.M{"$sort": sort})
	}

	// Limit stage
	if q.Limit != nil {
		pipeline = append(pipeline, bson.M{"$limit": *q.Limit})
	}

	// Skip stage
	if q.Offset != nil {
		pipeline = append(pipeline, bson.M{"$skip": *q.Offset})
	}

	return pipeline
}

func (t *MongoDBTranslator) buildAggregatePipeline(q *query.Query) []primitive.M {
	return t.buildPipeline(q)
}
