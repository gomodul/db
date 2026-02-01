package translator

import (
	"errors"

	"github.com/gomodul/db/query"
)

// Translator converts universal queries to backend-specific format
type Translator interface {
	// Translate converts universal query to backend-specific query
	Translate(q *query.Query) (BackendQuery, error)

	// Validate checks if query is supported by backend
	Validate(q *query.Query) error

	// Name returns the translator name
	Name() string
}

// BackendQuery is a marker interface for backend-specific queries
type BackendQuery interface{}

// BaseTranslator provides common functionality for translators
type BaseTranslator struct {
	name string
}

// NewBaseTranslator creates a new base translator
func NewBaseTranslator(name string) *BaseTranslator {
	return &BaseTranslator{name: name}
}

// Name returns the translator name
func (t *BaseTranslator) Name() string {
	return t.name
}

// Validate checks if the query is valid
// This is a default implementation that can be overridden
func (t *BaseTranslator) Validate(q *query.Query) error {
	// Basic validation
	if q.Collection == "" && q.Model == nil {
		return ErrInvalidQuery
	}
	return nil
}

var (
	// ErrInvalidQuery is returned when the query is invalid
	ErrInvalidQuery = errors.New("translator: invalid query")

	// ErrUnsupportedOperation is returned when the operation is not supported
	ErrUnsupportedOperation = errors.New("translator: unsupported operation")

	// ErrUnsupportedFilter is returned when a filter is not supported
	ErrUnsupportedFilter = errors.New("translator: unsupported filter")

	// ErrUnsupportedJoin is returned when a join is not supported
	ErrUnsupportedJoin = errors.New("translator: unsupported join")

	// ErrUnsupportedAggregation is returned when an aggregation is not supported
	ErrUnsupportedAggregation = errors.New("translator: unsupported aggregation")
)
