package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// Driver implements the dialect.Driver interface for Elasticsearch
type Driver struct {
	client *elasticsearch.Client
	config *dialect.Config
}

// NewDriver creates a new Elasticsearch driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "elasticsearch"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeSearch
}

// Initialize initializes the Elasticsearch connection
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg

	esCfg := elasticsearch.Config{
		Addresses: []string{cfg.DSN},
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	// Ping the cluster
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.Ping(client.Ping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("elasticsearch ping error: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch ping error: %s", res.Status())
	}

	d.client = client
	return nil
}

// Close closes the connection
func (d *Driver) Close() error {
	// ES client doesn't need explicit closing
	return nil
}

// Execute executes a universal query
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	index := d.getIndexName(q)

	switch q.Operation {
	case query.OpFind:
		return d.executeSearch(ctx, index, q)
	case query.OpCreate:
		return d.executeCreate(ctx, index, q)
	case query.OpUpdate:
		return d.executeUpdate(ctx, index, q)
	case query.OpDelete:
		return d.executeDelete(ctx, index, q)
	case query.OpCount:
		return d.executeCount(ctx, index, q)
	case query.OpAggregate:
		return d.executeAggregate(ctx, index, q)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", q.Operation)
	}
}

// executeSearch executes a search query
func (d *Driver) executeSearch(ctx context.Context, index string, q *query.Query) (*dialect.Result, error) {
	esQuery := d.buildSearchQuery(q)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(esQuery); err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  &buf,
		Pretty: true,
	}

	res, err := req.Do(ctx, d.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch search error: %s", res.Status())
	}

	return d.parseSearchResponse(res)
}

// executeCreate creates a document
func (d *Driver) executeCreate(ctx context.Context, index string, q *query.Query) (*dialect.Result, error) {
	if q.Document == nil {
		return nil, fmt.Errorf("document required for create")
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(q.Document); err != nil {
		return nil, err
	}

	req := esapi.IndexRequest{
		Index:      index,
		Body:       &buf,
		DocumentID: d.getDocumentID(q),
		Pretty:     true,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, d.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch index error: %s", res.Status())
	}

	var response map[string]interface{}
	json.NewDecoder(res.Body).Decode(&response)

	resultID, _ := response["_id"].(string)

	return &dialect.Result{
		RowsAffected:  1,
		LastInsertID: resultID,
	}, nil
}

// executeUpdate updates documents
func (d *Driver) executeUpdate(ctx context.Context, index string, q *query.Query) (*dialect.Result, error) {
	esUpdate := d.buildUpdateQuery(q)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(esUpdate); err != nil {
		return nil, err
	}

	req := esapi.UpdateRequest{
		Index:      index,
		DocumentID: "_update_by_query",
		Body:       &buf,
		Pretty:     true,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, d.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch update error: %s", res.Status())
	}

	var response map[string]interface{}
	json.NewDecoder(res.Body).Decode(&response)

	updated, _ := response["updated"].(float64)

	return &dialect.Result{
		RowsAffected: int64(updated),
	}, nil
}

// executeDelete deletes documents
func (d *Driver) executeDelete(ctx context.Context, index string, q *query.Query) (*dialect.Result, error) {
	esQuery := d.buildSearchQuery(q)
	// Remove size to get count only
	delete(esQuery, "size")

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(esQuery); err != nil {
		return nil, err
	}

	req := esapi.DeleteByQueryRequest{
		Index: []string{index},
		Body:  &buf,
		Pretty: true,
	}

	res, err := req.Do(ctx, d.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch delete error: %s", res.Status())
	}

	var response map[string]interface{}
	json.NewDecoder(res.Body).Decode(&response)

	deleted, _ := response["deleted"].(float64)

	return &dialect.Result{
		RowsAffected: int64(deleted),
	}, nil
}

// executeCount counts documents
func (d *Driver) executeCount(ctx context.Context, index string, q *query.Query) (*dialect.Result, error) {
	esQuery := map[string]interface{}{
		"query": d.buildQueryClause(q.Filters),
		"size":  0,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(esQuery); err != nil {
		return nil, err
	}

	req := esapi.CountRequest{
		Index: []string{index},
		Body:  &buf,
		Pretty: true,
	}

	res, err := req.Do(ctx, d.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch count error: %s", res.Status())
	}

	var response map[string]interface{}
	json.NewDecoder(res.Body).Decode(&response)

	count, _ := response["count"].(float64)

	return &dialect.Result{
		Count: int64(count),
	}, nil
}

// executeAggregate executes an aggregation query
func (d *Driver) executeAggregate(ctx context.Context, index string, q *query.Query) (*dialect.Result, error) {
	esQuery := map[string]interface{}{
		"size": 0,
		"aggs": d.buildAggregations(q),
	}

	if len(q.Filters) > 0 {
		esQuery["query"] = d.buildQueryClause(q.Filters)
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(esQuery); err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  &buf,
		Pretty: true,
	}

	res, err := req.Do(ctx, d.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch aggregation error: %s", res.Status())
	}

	return d.parseSearchResponse(res)
}

// Ping checks if Elasticsearch is reachable
func (d *Driver) Ping(ctx context.Context) error {
	res, err := d.client.Ping(d.client.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("ping error: %s", res.Status())
	}
	return nil
}

// Health returns health status
func (d *Driver) Health() (*dialect.HealthStatus, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.Ping(ctx); err != nil {
		return dialect.NewUnhealthyStatus(err.Error()), nil
	}

	// Get cluster health
	req := esapi.ClusterHealthRequest{
		Pretty: true,
	}

	res, err := req.Do(ctx, d.client)
	if err != nil {
		return dialect.NewHealthyStatus(time.Since(start)), nil
	}
	defer res.Body.Close()

	var health map[string]interface{}
	json.NewDecoder(res.Body).Decode(&health)

	status, _ := health["status"].(string)

	return dialect.NewHealthyStatus(time.Since(start)).WithDetail("cluster_status", status), nil
}

// Capabilities returns the driver's capabilities
func (d *Driver) Capabilities() *dialect.Capabilities {
	return &dialect.Capabilities{
		Query: dialect.QueryCapabilities{
			Create: true,
			Read:   true,
			Update: true,
			Delete: true,
			Filters: []query.FilterOperator{
				query.OpEqual,
				query.OpNotEqual,
				query.OpGreaterThan,
				query.OpGreaterOrEqual,
				query.OpLessThan,
				query.OpLessOrEqual,
				query.OpIn,
				query.OpNotIn,
				query.OpLike,
				query.OpNotLike,
				query.OpContains,
				query.OpStartsWith,
				query.OpEndsWith,
				query.OpNull,
				query.OpNotNull,
				query.OpBetween,
				query.OpRegex,
			},
			Sort:           true,
			MultiFieldSort: true,
			OffsetPagination: false,
			CursorPagination: true,
			GroupBy:          true,
			Aggregations: []query.AggOperator{
				query.AggOpCount,
				query.AggOpSum,
				query.AggOpAvg,
				query.AggOpMin,
				query.AggOpMax,
			},
			FullTextSearch: true,
			Geospatial:      true,
		},
		Schema: dialect.SchemaCapabilities{
			CreateIndexes: true,
			DropIndexes:  true,
		},
		Indexing: dialect.IndexCapabilities{
			Unique:   false,
			Composite: true,
			Partial:  true,
			FullText: true,
		},
	}
}

// Helper methods

func (d *Driver) getIndexName(q *query.Query) string {
	if q.Collection != "" {
		return q.Collection
	}
	if d.config != nil && d.config.Database != "" {
		return d.config.Database
	}
	return "default"
}

func (d *Driver) getDocumentID(q *query.Query) string {
	if q.Document != nil {
		if m, ok := q.Document.(map[string]interface{}); ok {
			if id, exists := m["_id"]; exists {
				return fmt.Sprintf("%v", id)
			}
			if id, exists := m["id"]; exists {
				return fmt.Sprintf("%v", id)
			}
		}
	}
	return ""
}

func (d *Driver) parseSearchResponse(res *esapi.Response) (*dialect.Result, error) {
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch error: %s", res.Status())
	}

	var response map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, err
	}

	// Extract hits
	hits, ok := response["hits"].(map[string]interface{})["hits"].([]interface{})
	if !ok {
		return &dialect.Result{Data: []interface{}{}}, nil
	}

	data := make([]interface{}, 0, len(hits))
	for _, hit := range hits {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}
		// Extract source document
		if source, ok := hitMap["_source"].(map[string]interface{}); ok {
			// Add ID to source
			source["_id"] = hitMap["_id"]
			data = append(data, source)
		}
	}

	// Get total count
	var count int64
	if total, ok := response["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64); ok {
		count = int64(total)
	}

	return &dialect.Result{
		Data:        data,
		Count:       count,
		RowsAffected: int64(len(data)),
	}, nil
}

func (d *Driver) buildSearchQuery(q *query.Query) map[string]interface{} {
	queryMap := map[string]interface{}{
		"query": d.buildQueryClause(q.Filters),
	}

	// Build sorting
	if len(q.Orders) > 0 {
		sort := make([]map[string]interface{}, 0, len(q.Orders))
		for _, order := range q.Orders {
			orderMap := map[string]interface{}{
				order.Field: order.Direction,
			}
			sort = append(sort, orderMap)
		}
		queryMap["sort"] = sort
	}

	// Add pagination
	if q.Limit != nil {
		queryMap["size"] = *q.Limit
	}
	if q.Offset != nil {
		queryMap["from"] = *q.Offset
	}

	// Add source filtering
	if len(q.Selects) > 0 {
		queryMap["_source"] = q.Selects
	}

	return queryMap
}

func (d *Driver) buildQueryClause(filters []*query.Filter) map[string]interface{} {
	if len(filters) == 0 {
		return map[string]interface{}{
			"match_all": map[string]interface{}{},
		}
	}

	must := make([]map[string]interface{}, 0)

	for _, filter := range filters {
		clause := d.buildFilterClause(filter)
		if clause != nil {
			must = append(must, clause)
		}
	}

	return map[string]interface{}{
		"bool": map[string]interface{}{
			"must": must,
		},
	}
}

func (d *Driver) buildFilterClause(filter *query.Filter) map[string]interface{} {
	if len(filter.Nested) > 0 {
		// Handle nested filters
		switch filter.Logic {
		case query.LogicOr:
			should := make([]map[string]interface{}, 0)
			for _, nested := range filter.Nested {
				clause := d.buildFilterClause(nested)
				if clause != nil {
					should = append(should, clause)
				}
			}
			return map[string]interface{}{
				"bool": map[string]interface{}{
					"should": should,
				},
			}
		case query.LogicNot:
			mustNot := make([]map[string]interface{}, 0)
			for _, nested := range filter.Nested {
				clause := d.buildFilterClause(nested)
				if clause != nil {
					mustNot = append(mustNot, clause)
				}
			}
			return map[string]interface{}{
				"bool": map[string]interface{}{
					"must_not": mustNot,
				},
			}
		default:
			must := make([]map[string]interface{}, 0)
			for _, nested := range filter.Nested {
				clause := d.buildFilterClause(nested)
				if clause != nil {
					must = append(must, clause)
				}
			}
			return map[string]interface{}{
				"bool": map[string]interface{}{
					"must": must,
				},
			}
		}
	}

	return d.buildSingleFilter(filter)
}

func (d *Driver) buildSingleFilter(filter *query.Filter) map[string]interface{} {
	clause := make(map[string]interface{})

	switch filter.Operator {
	case query.OpEqual:
		clause["term"] = map[string]interface{}{
			filter.Field: filter.Value,
		}
	case query.OpNotEqual:
		clause["bool"] = map[string]interface{}{
			"must_not": []map[string]interface{}{
				{"term": map[string]interface{}{filter.Field: filter.Value}},
			},
		}
	case query.OpGreaterThan:
		clause["range"] = map[string]interface{}{
			filter.Field: map[string]interface{}{
				"gt": filter.Value,
			},
		}
	case query.OpGreaterOrEqual:
		clause["range"] = map[string]interface{}{
			filter.Field: map[string]interface{}{
				"gte": filter.Value,
			},
		}
	case query.OpLessThan:
		clause["range"] = map[string]interface{}{
			filter.Field: map[string]interface{}{
				"lt": filter.Value,
			},
		}
	case query.OpLessOrEqual:
		clause["range"] = map[string]interface{}{
			filter.Field: map[string]interface{}{
				"lte": filter.Value,
			},
		}
	case query.OpIn:
		clause["terms"] = map[string]interface{}{
			filter.Field: filter.Values,
		}
	case query.OpLike:
		clause["wildcard"] = map[string]interface{}{
			filter.Field: "*" + fmt.Sprintf("%v", filter.Value) + "*",
		}
	case query.OpRegex:
		clause["regexp"] = map[string]interface{}{
			filter.Field: fmt.Sprintf("%v", filter.Value),
		}
	case query.OpNull:
		clause["bool"] = map[string]interface{}{
			"must_not": []map[string]interface{}{
				{"exists": map[string]interface{}{"field": filter.Field}},
			},
		}
	case query.OpNotNull:
		clause["exists"] = map[string]interface{}{
			"field": filter.Field,
		}
	default:
		clause["match"] = map[string]interface{}{
			filter.Field: filter.Value,
		}
	}

	return clause
}

func (d *Driver) buildUpdateQuery(q *query.Query) map[string]interface{} {
	update := map[string]interface{}{
		"query": d.buildQueryClause(q.Filters),
	}

	if len(q.Updates) > 0 {
		update["doc"] = q.Updates
	}

	// Use script for partial updates
	if len(q.Updates) > 0 {
		script := ""
		for field := range q.Updates {
			if script != "" {
				script += ", "
			}
			script += fmt.Sprintf("ctx._source.%s = params.%s", field, field)
		}
		update["script"] = map[string]interface{}{
			"source": script,
			"lang":   "painless",
		}
		update["params"] = q.Updates
		update["upsert"] = q.Hints != nil && q.Hints["upsert"] == true
	}

	return update
}

func (d *Driver) buildAggregations(q *query.Query) map[string]interface{} {
	aggs := make(map[string]interface{})

	for _, agg := range q.Aggregates {
		var aggBody interface{}

		switch agg.Operator {
		case query.AggOpCount:
			aggBody = map[string]interface{}{
				"value_count": map[string]interface{}{
					"field": agg.Field,
				},
			}
		case query.AggOpSum:
			aggBody = map[string]interface{}{
				"sum": map[string]interface{}{
					"field": agg.Field,
				},
			}
		case query.AggOpAvg:
			aggBody = map[string]interface{}{
				"avg": map[string]interface{}{
					"field": agg.Field,
				},
			}
		case query.AggOpMin:
			aggBody = map[string]interface{}{
				"min": map[string]interface{}{
					"field": agg.Field,
				},
			}
		case query.AggOpMax:
			aggBody = map[string]interface{}{
				"max": map[string]interface{}{
					"field": agg.Field,
				},
			}
		}

		aggs[agg.Alias] = aggBody
	}

	return aggs
}

func init() {
	// Register the Elasticsearch driver
	dialect.Register("elasticsearch", func() dialect.Driver {
		return NewDriver()
	})
	dialect.Register("elastic", func() dialect.Driver {
		return NewDriver()
	})
}
