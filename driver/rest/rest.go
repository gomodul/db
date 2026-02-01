package rest

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

// Driver implements the dialect.Driver interface for REST APIs
type Driver struct {
	client *http.Client
	config *dialect.Config
}

// NewDriver creates a new REST driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "rest"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeAPI
}

// Initialize initializes the REST client
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg

	d.client = &http.Client{
		Timeout: 30 * time.Second,
	}

	// Verify connection
	return d.ping()
}

// Close closes the REST client
func (d *Driver) Close() error {
	d.client = nil
	return nil
}

// Execute executes a universal query
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	url := d.buildURL(q)

	switch q.Operation {
	case query.OpFind:
		return d.executeGet(ctx, url, q)
	case query.OpCreate:
		return d.executePost(ctx, url, q)
	case query.OpUpdate:
		return d.executePut(ctx, url, q)
	case query.OpDelete:
		return d.executeDelete(ctx, url)
	case query.OpCount:
		return d.executeCount(ctx, url, q)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", q.Operation)
	}
}

// executeGet executes a GET request
func (d *Driver) executeGet(ctx context.Context, url string, q *query.Query) (*dialect.Result, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	d.addHeaders(req, q)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("REST API error: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	// Normalize result to array
	data := d.normalizeResult(result)

	return &dialect.Result{
		Data:        data,
		RowsAffected: int64(len(data)),
	}, nil
}

// executePost executes a POST request
func (d *Driver) executePost(ctx context.Context, url string, q *query.Query) (*dialect.Result, error) {
	body, err := json.Marshal(q.Document)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	d.addHeaders(req, q)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("REST API error: %s", resp.Status)
	}

	respBody, _ := io.ReadAll(resp.Body)

	// Try to parse response
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		return &dialect.Result{
			RowsAffected: 1,
			Data:        []interface{}{result},
		}, nil
	}

	return &dialect.Result{
		RowsAffected: 1,
	}, nil
}

// executePut executes a PUT request
func (d *Driver) executePut(ctx context.Context, url string, q *query.Query) (*dialect.Result, error) {
	body, err := json.Marshal(q.Updates)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	d.addHeaders(req, q)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("REST API error: %s", resp.Status)
	}

	return &dialect.Result{
		RowsAffected: 1,
	}, nil
}

// executeDelete executes a DELETE request
func (d *Driver) executeDelete(ctx context.Context, url string) (*dialect.Result, error) {
	req, err := http.NewRequestWithContext(context.Background(), "DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("REST API error: %s", resp.Status)
	}

	return &dialect.Result{
		RowsAffected: 1,
	}, nil
}

// executeCount executes a COUNT request
func (d *Driver) executeCount(ctx context.Context, url string, q *query.Query) (*dialect.Result, error) {
	// Add count parameter to URL
	countURL := url + "?count=true"

	req, err := http.NewRequestWithContext(ctx, "GET", countURL, nil)
	if err != nil {
		return nil, err
	}

	d.addHeaders(req, q)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("REST API error: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	count := int64(0)
	if c, ok := result["count"].(float64); ok {
		count = int64(c)
	} else if c, ok := result["total"].(float64); ok {
		count = int64(c)
	}

	return &dialect.Result{
		Count: count,
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
				query.OpContains,
				query.OpStartsWith,
				query.OpEndsWith,
				query.OpNull,
				query.OpNotNull,
			},
			Sort:           true,
			MultiFieldSort: true,
			OffsetPagination: true,
			CursorPagination: true,
		},
	}
}

// Ping checks if the REST API is reachable
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

// BeginTx returns error - REST APIs don't support transactions
func (d *Driver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	return nil, dialect.ErrNotSupported
}

// Helper methods

func (d *Driver) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", d.config.DSN, nil)
	if err != nil {
		return err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("REST API ping error: %s", resp.Status)
	}

	return nil
}

func (d *Driver) buildURL(q *query.Query) string {
	url := d.config.DSN

	// Add collection/resource path
	if q.Collection != "" {
		url = fmt.Sprintf("%s/%s", url, q.Collection)
	}

	// Add ID if present in filters
	for _, filter := range q.Filters {
		if filter.Field == "id" || filter.Field == "_id" {
			url = fmt.Sprintf("%s/%v", url, filter.Value)
			break
		}
	}

	// Add query parameters for filters
	if len(q.Filters) > 0 {
		url += "?"
		first := true
		for _, filter := range q.Filters {
			if filter.Field == "id" || filter.Field == "_id" {
				continue
			}
			if !first {
				url += "&"
			}
			first = false

			switch filter.Operator {
			case query.OpEqual:
				url += fmt.Sprintf("%s=%v", filter.Field, filter.Value)
			case query.OpNotEqual:
				url += fmt.Sprintf("%s[ne]=%v", filter.Field, filter.Value)
			case query.OpGreaterThan:
				url += fmt.Sprintf("%s[gt]=%v", filter.Field, filter.Value)
			case query.OpLessThan:
				url += fmt.Sprintf("%s[lt]=%v", filter.Field, filter.Value)
			case query.OpIn:
				for i, v := range filter.Values {
					if i > 0 {
						url += "&"
					}
					url += fmt.Sprintf("%s[in]=%v", filter.Field, v)
				}
			case query.OpContains:
				url += fmt.Sprintf("%s[contains]=%v", filter.Field, filter.Value)
			case query.OpStartsWith:
				url += fmt.Sprintf("%s[startsWith]=%v", filter.Field, filter.Value)
			case query.OpEndsWith:
				url += fmt.Sprintf("%s[endsWith]=%v", filter.Field, filter.Value)
			default:
				url += fmt.Sprintf("%s=%v", filter.Field, filter.Value)
			}
		}

		// Add pagination
		if q.Limit != nil {
			url += fmt.Sprintf("&limit=%d", *q.Limit)
		}
		if q.Offset != nil {
			url += fmt.Sprintf("&offset=%d", *q.Offset)
		}

		// Add sorting
		if len(q.Orders) > 0 {
			for _, order := range q.Orders {
				url += fmt.Sprintf("&sort=%s:%s", order.Field, order.Direction)
			}
		}
	}

	return url
}

func (d *Driver) addHeaders(req *http.Request, q *query.Query) {
	// Add auth header if provided in options
	if d.config.Options != nil {
		if auth, ok := d.config.Options["auth"].(string); ok {
			req.Header.Set("Authorization", auth)
		}
		if token, ok := d.config.Options["token"].(string); ok {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	// Add custom headers from hints
	if q.Hints != nil {
		if headers, ok := q.Hints["headers"].(map[string]string); ok {
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}
	}
}

func (d *Driver) normalizeResult(result interface{}) []interface{} {
	switch v := result.(type) {
	case []interface{}:
		return v
	case map[string]interface{}:
		// Check for common response wrappers
		if data, ok := v["data"].([]interface{}); ok {
			return data
		}
		if items, ok := v["items"].([]interface{}); ok {
			return items
		}
		if results, ok := v["results"].([]interface{}); ok {
			return results
		}
		return []interface{}{v}
	default:
		return []interface{}{result}
	}
}

func init() {
	// Register the REST driver
	dialect.Register("rest", func() dialect.Driver {
		return NewDriver()
	})
	dialect.Register("http", func() dialect.Driver {
		return NewDriver()
	})
	dialect.Register("https", func() dialect.Driver {
		return NewDriver()
	})
}
