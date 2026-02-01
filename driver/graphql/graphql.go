package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// Driver implements the dialect.Driver interface for GraphQL APIs
type Driver struct {
	client *http.Client
	config *dialect.Config
}

// NewDriver creates a new GraphQL driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "graphql"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeGraphQL
}

// Initialize initializes the GraphQL client
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg

	d.client = &http.Client{
		Timeout: 30 * time.Second,
	}

	// Verify connection
	return d.ping()
}

// Close closes the GraphQL client
func (d *Driver) Close() error {
	d.client = nil
	return nil
}

// Execute executes a universal query
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	switch q.Operation {
	case query.OpFind:
		return d.executeQuery(ctx, q)
	case query.OpCreate:
		return d.executeMutation(ctx, q, "create")
	case query.OpUpdate:
		return d.executeMutation(ctx, q, "update")
	case query.OpDelete:
		return d.executeMutation(ctx, q, "delete")
	default:
		return nil, fmt.Errorf("unsupported operation: %s", q.Operation)
	}
}

// executeQuery executes a GraphQL query
func (d *Driver) executeQuery(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	gqlQuery := d.buildQuery(q)

	payload := map[string]interface{}{
		"query": gqlQuery,
		"variables": d.buildVariables(q),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.config.DSN, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	d.addHeaders(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GraphQL API error: %s", resp.Status)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response map[string]interface{}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, err
	}

	// Extract data from GraphQL response
	data := d.extractData(response, q)

	return &dialect.Result{
		Data:        data,
		RowsAffected: int64(len(data)),
	}, nil
}

// executeMutation executes a GraphQL mutation
func (d *Driver) executeMutation(ctx context.Context, q *query.Query, operation string) (*dialect.Result, error) {
	gqlMutation := d.buildMutation(q, operation)

	payload := map[string]interface{}{
		"query": gqlMutation,
		"variables": d.buildVariables(q),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.config.DSN, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	d.addHeaders(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GraphQL API error: %s", resp.Status)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response map[string]interface{}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, err
	}

	// Extract data from GraphQL response
	data := d.extractData(response, q)

	return &dialect.Result{
		Data:        data,
		RowsAffected: 1,
	}, nil
}

// Capabilities returns the driver's capabilities
func (d *Driver) Capabilities() *dialect.Capabilities {
	return &dialect.Capabilities{
		Query: dialect.QueryCapabilities{
			Create:      true,
			Read:        true,
			Update:      true,
			Delete:      true,
			BatchCreate: true,
			Filters: []query.FilterOperator{
				query.OpEqual,
				query.OpNotEqual,
				query.OpGreaterThan,
				query.OpLessThan,
				query.OpIn,
				query.OpNotIn,
			},
			Sort:           true,
			MultiFieldSort: true,
			OffsetPagination: true,
		},
	}
}

// Ping checks if the GraphQL API is reachable
func (d *Driver) Ping(ctx context.Context) error {
	return d.ping()
}

// Health returns the health status
func (d *Driver) Health() (*dialect.HealthStatus, error) {
	start := time.Now()

	if err := d.ping(); err != nil {
		return dialect.NewUnhealthyStatus(err.Error()), nil
	}

	return dialect.NewHealthyStatus(time.Since(start)), nil
}

// BeginTx returns error - GraphQL APIs don't support transactions
func (d *Driver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	return nil, dialect.ErrNotSupported
}

// Helper methods

func (d *Driver) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simple introspection query to verify connection
	query := `{ __typename }`

	payload := map[string]interface{}{
		"query": query,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.config.DSN, bytes.NewReader(body))
	if err != nil {
		return err
	}

	d.addHeaders(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GraphQL API ping error: %s", resp.Status)
	}

	return nil
}

func (d *Driver) buildQuery(q *query.Query) string {
	var builder queryBuilder

	collection := q.Collection
	if collection == "" {
		collection = "items"
	}

	// Build selection fields
	fields := d.buildSelectionFields(q)

	builder.WriteString("query { ")
	builder.WriteString(collection)

	// Add arguments
	if len(q.Filters) > 0 || q.Limit != nil || len(q.Orders) > 0 {
		builder.WriteString("(")
		first := true

		// Add filters
		if len(q.Filters) > 0 {
			builder.WriteString("where: {")
			for i, filter := range q.Filters {
				if i > 0 {
					builder.WriteString(", ")
				}
				builder.WriteString(filter.Field)
				builder.WriteString(": ")
				builder.WriteString(formatValue(filter.Value))
			}
			builder.WriteString("}")
			first = false
		}

		// Add limit
		if q.Limit != nil {
			if !first {
				builder.WriteString(", ")
			}
			builder.WriteString(fmt.Sprintf("limit: %d", *q.Limit))
			first = false
		}

		// Add offset
		if q.Offset != nil {
			if !first {
				builder.WriteString(", ")
			}
			builder.WriteString(fmt.Sprintf("offset: %d", *q.Offset))
		}

		// Add order
		if len(q.Orders) > 0 {
			if !first {
				builder.WriteString(", ")
			}
			builder.WriteString("orderBy: [")
			for i, order := range q.Orders {
				if i > 0 {
					builder.WriteString(", ")
				}
				builder.WriteString(fmt.Sprintf("{%s: %s}", order.Field, order.Direction))
			}
			builder.WriteString("]")
		}

		builder.WriteString(")")
	}

	// Add fields
	builder.WriteString(" { ")
	builder.WriteString(fields)
	builder.WriteString(" }")
	builder.WriteString(" }")

	return builder.String()
}

func (d *Driver) buildMutation(q *query.Query, operation string) string {
	collection := q.Collection
	if collection == "" {
		collection = "items"
	}

	mutationName := fmt.Sprintf("%s%s", operation, collection)

	var builder queryBuilder
	builder.WriteString("mutation { ")
	builder.WriteString(mutationName)

	// Add input
	if operation == "create" && q.Document != nil {
		builder.WriteString("(input: ")
		builder.WriteString(formatValue(q.Document))
		builder.WriteString(")")
	} else if operation == "update" && len(q.Updates) > 0 {
		builder.WriteString("(id: ")
		// Try to get ID from filters
		for _, filter := range q.Filters {
			if filter.Field == "id" || filter.Field == "_id" {
				builder.WriteString(formatValue(filter.Value))
				break
			}
		}
		builder.WriteString(", input: ")
		builder.WriteString(formatValue(q.Updates))
		builder.WriteString(")")
	} else if operation == "delete" {
		builder.WriteString("(id: ")
		for _, filter := range q.Filters {
			if filter.Field == "id" || filter.Field == "_id" {
				builder.WriteString(formatValue(filter.Value))
				break
			}
		}
		builder.WriteString(")")
	}

	builder.WriteString(" { ")
	builder.WriteString("id ")
	builder.WriteString("} ")
	builder.WriteString("}")

	return builder.String()
}

func (d *Driver) buildSelectionFields(q *query.Query) string {
	if len(q.Selects) > 0 {
		var builder queryBuilder
		for i, field := range q.Selects {
			if i > 0 {
				builder.WriteString(" ")
			}
			builder.WriteString(field)
		}
		return builder.String()
	}
	return "id"
}

func (d *Driver) buildVariables(q *query.Query) map[string]interface{} {
	vars := make(map[string]interface{})

	if q.Document != nil {
		vars["input"] = q.Document
	}

	if len(q.Updates) > 0 {
		vars["input"] = q.Updates
	}

	return vars
}

func (d *Driver) addHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")

	// Add auth header if provided in options
	if d.config.Options != nil {
		if auth, ok := d.config.Options["auth"].(string); ok {
			req.Header.Set("Authorization", auth)
		}
		if token, ok := d.config.Options["token"].(string); ok {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
}

func (d *Driver) extractData(response map[string]interface{}, q *query.Query) []interface{} {
	// GraphQL response format: { "data": { "collection": [...] } }
	if data, ok := response["data"].(map[string]interface{}); ok {
		collection := q.Collection
		if collection == "" {
			collection = "items"
		}

		if items, ok := data[collection].([]interface{}); ok {
			return items
		}

		if item, ok := data[collection]; ok {
			return []interface{}{item}
		}
	}

	return []interface{}{}
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf(`"%s"`, val)
	case map[string]interface{}:
		var builder queryBuilder
		builder.WriteString("{")
		first := true
		for k, v := range val {
			if !first {
				builder.WriteString(", ")
			}
			first = false
			builder.WriteString(fmt.Sprintf("%s: %s", k, formatValue(v)))
		}
		builder.WriteString("}")
		return builder.String()
	case []interface{}:
		var builder queryBuilder
		builder.WriteString("[")
		for i, v := range val {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(formatValue(v))
		}
		builder.WriteString("]")
		return builder.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}

type queryBuilder struct {
	bytes.Buffer
}

func (qb *queryBuilder) WriteString(s string) {
	qb.Buffer.WriteString(s)
}

func init() {
	// Register the GraphQL driver
	dialect.Register("graphql", func() dialect.Driver {
		return NewDriver()
	})
	dialect.Register("gql", func() dialect.Driver {
		return NewDriver()
	})
}
