package translator

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/gomodul/db/query"
)

// RESTTranslator translates universal queries to HTTP requests
type RESTTranslator struct {
	*BaseTranslator
	baseURL    string
	authHeader string
}

// NewRESTTranslator creates a new REST translator
func NewRESTTranslator(baseURL string) *RESTTranslator {
	return &RESTTranslator{
		BaseTranslator: NewBaseTranslator("rest"),
		baseURL:        strings.TrimSuffix(baseURL, "/"),
	}
}

// SetAuthHeader sets the authorization header
func (t *RESTTranslator) SetAuthHeader(header string) {
	t.authHeader = header
}

// RESTRequest represents a translated REST API request
type RESTRequest struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    interface{}
	Query   url.Values
}

// Translate converts universal query to REST request
func (t *RESTTranslator) Translate(q *query.Query) (BackendQuery, error) {
	if err := t.Validate(q); err != nil {
		return nil, err
	}

	req := &RESTRequest{
		Headers: make(map[string]string),
		Query:   make(url.Values),
	}

	if t.authHeader != "" {
		req.Headers["Authorization"] = t.authHeader
	}
	req.Headers["Content-Type"] = "application/json"
	req.Headers["Accept"] = "application/json"

	// Build URL path
	resourcePath := t.buildResourcePath(q)
	req.URL = t.baseURL + "/" + resourcePath

	switch q.Operation {
	case query.OpFind:
		req.Method = "GET"
		t.buildQueryParams(q, req.Query)
		t.buildPagination(q, req.Query)

	case query.OpCreate:
		req.Method = "POST"
		req.Body = q.Document

	case query.OpUpdate:
		req.Method = "PUT" // or PATCH depending on API design
		t.buildQueryParams(q, req.Query)
		req.Body = q.Updates

	case query.OpDelete:
		req.Method = "DELETE"
		t.buildQueryParams(q, req.Query)

	case query.OpCount:
		req.Method = "GET"
		req.Query.Set("_count", "true")
		t.buildQueryParams(q, req.Query)

	case query.OpAggregate:
		req.Method = "POST"
		req.URL = req.URL + "/_aggregate"
		req.Body = t.buildAggregateBody(q)

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, q.Operation)
	}

	// Append query string to URL
	if len(req.Query) > 0 {
		req.URL += "?" + req.Query.Encode()
	}

	return req, nil
}

func (t *RESTTranslator) buildResourcePath(q *query.Query) string {
	if q.Collection != "" {
		return q.Collection
	}
	return modelCollectionName(q.Model)
}

func (t *RESTTranslator) buildQueryParams(q *query.Query, params url.Values) {
	// Build filter parameters
	for _, filter := range q.Filters {
		t.buildFilterParam(filter, params)
	}

	// Build sorting
	for _, order := range q.Orders {
		sortKey := order.Field
		if order.Direction == query.DirDesc {
			sortKey = "-" + sortKey
		}
		params.Add("sort", sortKey)
	}

	// Build field selection
	if len(q.Selects) > 0 {
		params.Add("fields", strings.Join(q.Selects, ","))
	}
}

func (t *RESTTranslator) buildFilterParam(filter *query.Filter, params url.Values) {
	if len(filter.Nested) > 0 {
		for _, nested := range filter.Nested {
			t.buildFilterParam(nested, params)
		}
		return
	}

	paramKey := filter.Field
	paramValue := fmt.Sprintf("%v", filter.Value)

	switch filter.Operator {
	case query.OpEqual:
		params.Set(paramKey, paramValue)
	case query.OpNotEqual:
		params.Set(paramKey+"_ne", paramValue)
	case query.OpGreaterThan:
		params.Set(paramKey+"_gt", paramValue)
	case query.OpGreaterOrEqual:
		params.Set(paramKey+"_gte", paramValue)
	case query.OpLessThan:
		params.Set(paramKey+"_lt", paramValue)
	case query.OpLessOrEqual:
		params.Set(paramKey+"_lte", paramValue)
	case query.OpIn:
		for _, v := range filter.Values {
			params.Add(paramKey, fmt.Sprintf("%v", v))
		}
	case query.OpLike:
		params.Set(paramKey+"_like", paramValue)
	case query.OpNull:
		params.Set(paramKey+"_null", "true")
	case query.OpNotNull:
		params.Set(paramKey+"_nnull", "true")
	}
}

func (t *RESTTranslator) buildPagination(q *query.Query, params url.Values) {
	if q.Limit != nil {
		params.Set("limit", fmt.Sprintf("%d", *q.Limit))
	}
	if q.Offset != nil {
		params.Set("offset", fmt.Sprintf("%d", *q.Offset))
	}
	if q.Cursor != nil {
		params.Set("cursor", fmt.Sprintf("%v", q.Cursor.Value))
	}
}

func (t *RESTTranslator) buildAggregateBody(q *query.Query) map[string]interface{} {
	body := make(map[string]interface{})

	if len(q.Filters) > 0 {
		filters := make(map[string]interface{})
		for _, filter := range q.Filters {
			t.buildFilterBody(filter, filters)
		}
		body["filter"] = filters
	}

	if len(q.Groups) > 0 {
		body["group_by"] = q.Groups
	}

	if len(q.Aggregates) > 0 {
		aggs := make([]map[string]interface{}, 0, len(q.Aggregates))
		for _, agg := range q.Aggregates {
			aggs = append(aggs, map[string]interface{}{
				"operator": agg.Operator,
				"field":    agg.Field,
				"alias":    agg.Alias,
			})
		}
		body["aggregations"] = aggs
	}

	return body
}

func (t *RESTTranslator) buildFilterBody(filter *query.Filter, body map[string]interface{}) {
	if len(filter.Nested) > 0 {
		for _, nested := range filter.Nested {
			t.buildFilterBody(nested, body)
		}
		return
	}

	body[filter.Field] = map[string]interface{}{
		"operator": filter.Operator,
		"value":    filter.Value,
	}
}
