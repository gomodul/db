package db

import (
	"context"
	"errors"
	"fmt"
)

var (
	// Base errors - use these with Errorf for context
	// ErrNotFound is returned when a record is not found.
	ErrNotFound = errors.New("db: not found")

	// ErrDuplicate is returned when a unique constraint is violated.
	ErrDuplicate = errors.New("db: duplicate entry")

	// ErrClosed is returned when operating on a closed connection.
	ErrClosed = errors.New("db: connection closed")

	// ErrTxDone is returned when a transaction has already been committed or rolled back.
	ErrTxDone = errors.New("db: transaction already done")

	// ErrNoDriver is returned when no driver is registered for the given engine.
	ErrNoDriver = errors.New("db: no driver registered")

	// ErrNotSupported is returned when an operation is not supported by the engine.
	ErrNotSupported = errors.New("db: operation not supported")

	// Additional specific errors
	ErrInvalidDSN      = errors.New("db: invalid DSN")
	ErrConnectionFailed = errors.New("db: connection failed")
	ErrQueryFailed     = errors.New("db: query failed")
	ErrTransactionFailed = errors.New("db: transaction failed")
	ErrValidationFailed = errors.New("db: validation failed")
	ErrMigrationFailed = errors.New("db: migration failed")
)

// DetailedError provides additional context for errors
type DetailedError struct {
	// Op is the operation being performed (e.g., "Find", "Create", "Update")
	Op string
	// Table is the table/collection being operated on
	Table string
	// Field is the specific field that caused the error (if applicable)
	Field string
	// Value is the value that caused the error (if applicable)
	Value interface{}
	// Err is the underlying error
	Err error
	// Query is the query that caused the error (if applicable)
	Query string
	// Driver is the driver being used
	Driver string
}

// Error implements the error interface
func (e *DetailedError) Error() string {
	var msg string
	if e.Op != "" {
		msg += e.Op + ": "
	}
	if e.Table != "" {
		msg += "table=" + e.Table + ", "
	}
	if e.Field != "" {
		msg += "field=" + e.Field + ", "
	}
	if e.Value != nil {
		msg += fmt.Sprintf("value=%v, ", e.Value)
	}
	if e.Query != "" {
		msg += "query=" + e.Query + ", "
	}
	if e.Driver != "" {
		msg += "driver=" + e.Driver + ", "
	}
	msg += "error=" + e.Err.Error()
	return msg
}

// Unwrap returns the underlying error for use with errors.Is/As
func (e *DetailedError) Unwrap() error {
	return e.Err
}

// NewDetailedError creates a new detailed error with context
func NewDetailedError(op, table string, err error) *DetailedError {
	return &DetailedError{
		Op:    op,
		Table: table,
		Err:   err,
	}
}

// WithField adds field context to the error
func (e *DetailedError) WithField(field string, value interface{}) *DetailedError {
	e.Field = field
	e.Value = value
	return e
}

// WithQuery adds query context to the error
func (e *DetailedError) WithQuery(query string) *DetailedError {
	e.Query = query
	return e
}

// WithDriver adds driver context to the error
func (e *DetailedError) WithDriver(driver string) *DetailedError {
	e.Driver = driver
	return e
}

// Errorf creates a new error with operation context
//
// Example:
//	return Errorf("Find", "users", "failed to find user by id: %w", err)
func Errorf(op, table, format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	return &DetailedError{
		Op:    op,
		Table: table,
		Err:   err,
	}
}

// WrapError wraps an error with additional context
//
// Example:
//	if err != nil {
//	    return WrapError(err, "Create", "users").WithField("email", user.Email)
//	}
func WrapError(err error, op, table string) *DetailedError {
	return &DetailedError{
		Op:    op,
		Table: table,
		Err:   err,
	}
}

// GetErrorContext extracts structured information from an error
type GetErrorContext struct {
	Operation string
	Table     string
	Field     string
	Query     string
	Driver    string
}

// ExtractErrorContext extracts context from an error if it's a DetailedError
func ExtractErrorContext(err error) *GetErrorContext {
	if err == nil {
		return nil
	}

	var detailedErr *DetailedError
	if errors.As(err, &detailedErr) {
		return &GetErrorContext{
			Operation: detailedErr.Op,
			Table:     detailedErr.Table,
			Field:     detailedErr.Field,
			Query:     detailedErr.Query,
			Driver:    detailedErr.Driver,
		}
	}

	return nil
}

// ContextError adds context to an error for better debugging
type ContextError struct {
	Err     error
	Context map[string]interface{}
}

// Error implements the error interface
func (e *ContextError) Error() string {
	msg := e.Err.Error()
	if len(e.Context) > 0 {
		msg += " ("
		first := true
		for k, v := range e.Context {
			if !first {
				msg += ", "
			}
			msg += fmt.Sprintf("%s=%v", k, v)
			first = false
		}
		msg += ")"
	}
	return msg
}

// Unwrap returns the underlying error
func (e *ContextError) Unwrap() error {
	return e.Err
}

// WithContext adds context to any error
func WithContext(err error, ctx map[string]interface{}) error {
	if err == nil {
		return nil
	}
	return &ContextError{
		Err:     err,
		Context: ctx,
	}
}

// WithErrorContext adds specific context key-value pairs to an error
func WithErrorContext(err error, keyvals ...interface{}) error {
	if err == nil {
		return nil
	}
	if len(keyvals)%2 != 0 {
		return fmt.Errorf("WithErrorContext requires even number of key-value pairs")
	}

	ctx := make(map[string]interface{})
	for i := 0; i < len(keyvals); i += 2 {
		if key, ok := keyvals[i].(string); ok {
			ctx[key] = keyvals[i+1]
		}
	}

	return &ContextError{
		Err:     err,
		Context: ctx,
	}
}

// IsErrNotFound returns true if the error is ErrNotFound.
func IsErrNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsErrDuplicate returns true if the error is ErrDuplicate.
func IsErrDuplicate(err error) bool {
	return errors.Is(err, ErrDuplicate)
}

// IsErrClosed returns true if the error is ErrClosed.
func IsErrClosed(err error) bool {
	return errors.Is(err, ErrClosed)
}

// IsErrTxDone returns true if the error is ErrTxDone.
func IsErrTxDone(err error) bool {
	return errors.Is(err, ErrTxDone)
}

// IsErrNoDriver returns true if the error is ErrNoDriver.
func IsErrNoDriver(err error) bool {
	return errors.Is(err, ErrNoDriver)
}

// IsErrNotSupported returns true if the error is ErrNotSupported.
func IsErrNotSupported(err error) bool {
	return errors.Is(err, ErrNotSupported)
}

// IsErrConnectionFailed returns true if the error is connection-related
func IsErrConnectionFailed(err error) bool {
	return errors.Is(err, ErrConnectionFailed) ||
		   errors.Is(err, ErrClosed) ||
		   errors.Is(err, context.DeadlineExceeded) ||
		   errors.Is(err, context.Canceled)
}

// IsRetryable returns true if the error might be retried
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for known retryable errors
	if errors.Is(err, ErrConnectionFailed) ||
	   errors.Is(err, context.DeadlineExceeded) ||
	   errors.Is(err, context.Canceled) {
		return true
	}

	// Check for detailed error with retryable underlying error
	var detailedErr *DetailedError
	if errors.As(err, &detailedErr) {
		return IsRetryable(detailedErr.Err)
	}

	var ctxErr *ContextError
	if errors.As(err, &ctxErr) {
		return IsRetryable(ctxErr.Err)
	}

	return false
}
