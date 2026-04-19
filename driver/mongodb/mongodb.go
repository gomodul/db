package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// Driver implements the dialect.Driver interface for MongoDB
type Driver struct {
	client   *mongo.Client
	database *mongo.Database
	config   *dialect.Config
}

// NewDriver creates a new MongoDB driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "mongodb"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeNoSQL
}

// Initialize initializes the MongoDB connection
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.DSN))
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb: %w", err)
	}

	// Ping the database
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongodb ping error: %w", err)
	}

	d.client = client

	// Set default database
	dbName := cfg.Database
	if dbName == "" {
		dbName = "test"
	}
	d.database = client.Database(dbName)

	return nil
}

// Close closes the MongoDB connection
func (d *Driver) Close() error {
	if d.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return d.client.Disconnect(ctx)
	}
	return nil
}

// Execute executes a universal query
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	collection := d.getCollection(q)

	switch q.Operation {
	case query.OpFind:
		return d.executeFind(ctx, collection, q)
	case query.OpCreate:
		return d.executeCreate(ctx, collection, q)
	case query.OpUpdate:
		return d.executeUpdate(ctx, collection, q)
	case query.OpDelete:
		return d.executeDelete(ctx, collection, q)
	case query.OpCount:
		return d.executeCount(ctx, collection, q)
	case query.OpAggregate:
		return d.executeAggregate(ctx, collection, q)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", q.Operation)
	}
}

// executeFind executes a find query
func (d *Driver) executeFind(ctx context.Context, collection *mongo.Collection, q *query.Query) (*dialect.Result, error) {
	filter := d.buildFilter(q.Filters)

	opts := options.Find()
	if q.Limit != nil {
		opts.SetLimit(int64(*q.Limit))
	}
	if q.Offset != nil {
		opts.SetSkip(int64(*q.Offset))
	}

	if len(q.Orders) > 0 {
		opts.SetSort(d.buildSort(q.Orders))
	}

	if len(q.Selects) > 0 {
		projection := make(bson.M)
		for _, field := range q.Selects {
			projection[field] = 1
		}
		opts.SetProjection(projection)
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []interface{}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return &dialect.Result{
		Data:        results,
		RowsAffected: int64(len(results)),
	}, nil
}

// executeCreate creates a document
func (d *Driver) executeCreate(ctx context.Context, collection *mongo.Collection, q *query.Query) (*dialect.Result, error) {
	if q.Document == nil {
		return nil, fmt.Errorf("document required for create")
	}

	// Convert document to BSON
	doc, ok := q.Document.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("document must be a map")
	}

	// Add _id if not present
	if _, exists := doc["_id"]; !exists {
		doc["_id"] = primitive.NewObjectID()
	}

	result, err := collection.InsertOne(ctx, doc)
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		RowsAffected:  1,
		LastInsertID: result.InsertedID.(primitive.ObjectID).Hex(),
	}, nil
}

// executeUpdate updates documents
func (d *Driver) executeUpdate(ctx context.Context, collection *mongo.Collection, q *query.Query) (*dialect.Result, error) {
	filter := d.buildFilter(q.Filters)

	update := d.buildUpdate(q)

	result, err := collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		RowsAffected: result.MatchedCount,
	}, nil
}

// executeDelete deletes documents
func (d *Driver) executeDelete(ctx context.Context, collection *mongo.Collection, q *query.Query) (*dialect.Result, error) {
	filter := d.buildFilter(q.Filters)

	result, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		RowsAffected: result.DeletedCount,
	}, nil
}

// executeCount counts documents
func (d *Driver) executeCount(ctx context.Context, collection *mongo.Collection, q *query.Query) (*dialect.Result, error) {
	filter := d.buildFilter(q.Filters)

	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		Count: count,
	}, nil
}

// executeAggregate executes an aggregation pipeline
func (d *Driver) executeAggregate(ctx context.Context, collection *mongo.Collection, q *query.Query) (*dialect.Result, error) {
	pipeline := d.buildPipeline(q)

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []interface{}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return &dialect.Result{
		Data:        results,
		RowsAffected: int64(len(results)),
	}, nil
}

// Capabilities returns the driver's capabilities
func (d *Driver) Capabilities() *dialect.Capabilities {
	return &dialect.Capabilities{
		Query: dialect.QueryCapabilities{
			Create:         true,
			Read:           true,
			Update:         true,
			Delete:         true,
			BatchCreate:    true,
			BatchUpdate:    true,
			BatchDelete:    true,
			Filters:        allFilterOperators(),
			Sort:           true,
			MultiFieldSort: true,
			OffsetPagination: true,
			CursorPagination: true,
			GroupBy:         true,
			Aggregations:    allAggregationOperators(),
			FullTextSearch:  true,
			Geospatial:      true,
		},
		Schema: dialect.SchemaCapabilities{
			CreateIndexes: true,
			DropIndexes:  true,
		},
		Indexing: dialect.IndexCapabilities{
			Unique:    true,
			Composite: true,
			Partial:   true,
			FullText:  true,
			Geospatial: true,
			Hash:      true,
		},
	}
}

// Ping checks if MongoDB is reachable
func (d *Driver) Ping(ctx context.Context) error {
	return d.client.Ping(ctx, nil)
}

// Health returns the health status
func (d *Driver) Health() (*dialect.HealthStatus, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.Ping(ctx); err != nil {
		return dialect.NewUnhealthyStatus(err.Error()), nil
	}

	return dialect.NewHealthyStatus(time.Since(start)), nil
}

// BeginTx starts a new transaction (MongoDB session)
func (d *Driver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	session, err := d.client.StartSession()
	if err != nil {
		return nil, err
	}
	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		return nil, err
	}
	return &MongoTx{session: session, ctx: ctx, driver: d}, nil
}

// MongoTx represents a MongoDB transaction
type MongoTx struct {
	session mongo.Session
	ctx     context.Context
	driver  *Driver
}

// Commit commits the transaction
func (t *MongoTx) Commit() error {
	return t.session.CommitTransaction(t.ctx)
}

// Rollback rolls back the transaction
func (t *MongoTx) Rollback() error {
	return t.session.AbortTransaction(t.ctx)
}

// Query executes a query within the transaction
func (t *MongoTx) Query(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	return t.driver.Execute(ctx, q)
}

// Exec executes a command within the transaction
func (t *MongoTx) Exec(ctx context.Context, rawSQL string, args ...interface{}) (*dialect.Result, error) {
	// MongoDB doesn't support raw SQL
	return nil, fmt.Errorf("raw SQL not supported for MongoDB")
}

// Helper methods

func (d *Driver) getCollection(q *query.Query) *mongo.Collection {
	collectionName := q.Collection
	if collectionName == "" {
		collectionName = "default"
	}
	return d.database.Collection(collectionName)
}

func (d *Driver) buildFilter(filters []*query.Filter) bson.M {
	if len(filters) == 0 {
		return bson.M{}
	}

	result := bson.M{}
	for _, filter := range filters {
		clause := d.buildFilterClause(filter)
		for k, v := range clause {
			result[k] = v
		}
	}
	return result
}

func (d *Driver) buildFilterClause(filter *query.Filter) bson.M {
	if len(filter.Nested) > 0 {
		// Handle nested filters
		switch filter.Logic {
		case query.LogicOr:
			or := make([]bson.M, 0)
			for _, nested := range filter.Nested {
				clause := d.buildFilterClause(nested)
				or = append(or, clause)
			}
			return bson.M{"$or": or}
		case query.LogicNot:
			not := make([]bson.M, 0)
			for _, nested := range filter.Nested {
				clause := d.buildFilterClause(nested)
				not = append(not, clause)
			}
			return bson.M{"$nor": not}
		default:
			and := make([]bson.M, 0)
			for _, nested := range filter.Nested {
				clause := d.buildFilterClause(nested)
				and = append(and, clause)
			}
			return bson.M{"$and": and}
		}
	}

	return d.buildSingleFilter(filter)
}

func (d *Driver) buildSingleFilter(filter *query.Filter) bson.M {
	clause := bson.M{}

	switch filter.Operator {
	case query.OpEqual:
		clause[filter.Field] = filter.Value
	case query.OpNotEqual:
		clause[filter.Field] = bson.M{"$ne": filter.Value}
	case query.OpGreaterThan:
		clause[filter.Field] = bson.M{"$gt": filter.Value}
	case query.OpGreaterOrEqual:
		clause[filter.Field] = bson.M{"$gte": filter.Value}
	case query.OpLessThan:
		clause[filter.Field] = bson.M{"$lt": filter.Value}
	case query.OpLessOrEqual:
		clause[filter.Field] = bson.M{"$lte": filter.Value}
	case query.OpIn:
		clause[filter.Field] = bson.M{"$in": filter.Values}
	case query.OpNotIn:
		clause[filter.Field] = bson.M{"$nin": filter.Values}
	case query.OpLike:
		clause[filter.Field] = bson.M{"$regex": fmt.Sprintf(".*%v.*", filter.Value), "$options": "i"}
	case query.OpStartsWith:
		clause[filter.Field] = bson.M{"$regex": fmt.Sprintf("^%v", filter.Value), "$options": "i"}
	case query.OpEndsWith:
		clause[filter.Field] = bson.M{"$regex": fmt.Sprintf("%v$", filter.Value), "$options": "i"}
	case query.OpContains:
		clause[filter.Field] = bson.M{"$in": filter.Values}
	case query.OpNull:
		clause[filter.Field] = bson.M{"$exists": false}
	case query.OpNotNull:
		clause[filter.Field] = bson.M{"$exists": true}
	case query.OpBetween:
		clause[filter.Field] = bson.M{"$gte": filter.Values[0], "$lte": filter.Values[1]}
	default:
		clause[filter.Field] = filter.Value
	}

	return clause
}

func (d *Driver) buildSort(orders []*query.Order) bson.D {
	sort := bson.D{}
	for _, order := range orders {
		direction := 1
		if order.Direction == "desc" || order.Direction == "DESC" {
			direction = -1
		}
		sort = append(sort, primitive.E{Key: order.Field, Value: direction})
	}
	return sort
}

func (d *Driver) buildUpdate(q *query.Query) bson.M {
	update := bson.M{"$set": q.Updates}
	return update
}

func (d *Driver) buildPipeline(q *query.Query) []bson.M {
	pipeline := []bson.M{}

	if len(q.Filters) > 0 {
		pipeline = append(pipeline, bson.M{"$match": d.buildFilter(q.Filters)})
	}

	if len(q.Aggregates) > 0 {
		group := bson.M{"_id": nil}
		for _, agg := range q.Aggregates {
			switch agg.Operator {
			case query.AggOpCount:
				group[agg.Alias] = bson.M{"$sum": 1}
			case query.AggOpSum:
				group[agg.Alias] = bson.M{"$sum": "$" + agg.Field}
			case query.AggOpAvg:
				group[agg.Alias] = bson.M{"$avg": "$" + agg.Field}
			case query.AggOpMin:
				group[agg.Alias] = bson.M{"$min": "$" + agg.Field}
			case query.AggOpMax:
				group[agg.Alias] = bson.M{"$max": "$" + agg.Field}
			}
		}
		pipeline = append(pipeline, bson.M{"$group": group})
	}

	if len(q.Orders) > 0 {
		pipeline = append(pipeline, bson.M{"$sort": d.buildSort(q.Orders)})
	}

	if q.Limit != nil {
		pipeline = append(pipeline, bson.M{"$limit": *q.Limit})
	}

	if q.Offset != nil {
		pipeline = append(pipeline, bson.M{"$skip": *q.Offset})
	}

	return pipeline
}

func allFilterOperators() []query.FilterOperator {
	return []query.FilterOperator{
		query.OpEqual,
		query.OpNotEqual,
		query.OpGreaterThan,
		query.OpGreaterOrEqual,
		query.OpLessThan,
		query.OpLessOrEqual,
		query.OpIn,
		query.OpNotIn,
		query.OpLike,
		query.OpContains,
		query.OpStartsWith,
		query.OpEndsWith,
		query.OpNull,
		query.OpNotNull,
		query.OpBetween,
	}
}

func allAggregationOperators() []query.AggOperator {
	return []query.AggOperator{
		query.AggOpCount,
		query.AggOpSum,
		query.AggOpAvg,
		query.AggOpMin,
		query.AggOpMax,
	}
}

// Migrator returns the dialect.Migrator for schema operations
func (d *Driver) Migrator() dialect.Migrator {
	return &Migrator{driver: d}
}

func init() {
	// Register the MongoDB driver
	dialect.Register("mongodb", func() dialect.Driver {
		return NewDriver()
	})
	dialect.Register("mongo", func() dialect.Driver {
		return NewDriver()
	})
}
