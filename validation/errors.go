package validation

import (
	"fmt"
	"strings"
)

// ErrorTypes represents different types of validation errors
type ErrorTypes int

const (
	// ErrorTypeRequired is for required field errors
	ErrorTypeRequired ErrorTypes = iota
	// ErrorTypeFormat is for format errors (email, url, etc.)
	ErrorTypeFormat
	// ErrorTypeRange is for range errors (min, max)
	ErrorTypeRange
	// ErrorTypeLength is for length errors
	ErrorTypeLength
	// ErrorTypeCustom is for custom validation errors
	ErrorTypeCustom
)

// FieldError represents a validation error for a specific field
type FieldError struct {
	Field   string
	Message string
	Type    ErrorTypes
	Param   string
}

// Error implements the error interface
func (e *FieldError) Error() string {
	if e.Param != "" {
		return fmt.Sprintf("%s %s (parameter: %s)", e.Field, e.Message, e.Param)
	}
	return fmt.Sprintf("%s %s", e.Field, e.Message)
}

// FieldErrors is a collection of field errors
type FieldErrors struct {
	Errors []*FieldError
}

// Error implements the error interface
func (e *FieldErrors) Error() string {
	if len(e.Errors) == 0 {
		return "no validation errors"
	}

	messages := make([]string, len(e.Errors))
	for i, err := range e.Errors {
		messages[i] = err.Error()
	}

	return fmt.Sprintf("validation failed: %s", strings.Join(messages, "; "))
}

// Add adds a new field error
func (e *FieldErrors) Add(field, message string, errorType ErrorTypes, param string) {
	e.Errors = append(e.Errors, &FieldError{
		Field:   field,
		Message: message,
		Type:    errorType,
		Param:   param,
	})
}

// HasErrors returns true if there are any errors
func (e *FieldErrors) HasErrors() bool {
	return len(e.Errors) > 0
}

// GetErrors returns all field errors
func (e *FieldErrors) GetErrors() []*FieldError {
	return e.Errors
}

// GetFieldErrors returns errors for a specific field
func (e *FieldErrors) GetFieldErrors(field string) []*FieldError {
	var errors []*FieldError
	for _, err := range e.Errors {
		if err.Field == field {
			errors = append(errors, err)
		}
	}
	return errors
}

// GetFields returns all field names that have errors
func (e *FieldErrors) GetFields() []string {
	fields := make(map[string]bool)
	for _, err := range e.Errors {
		fields[err.Field] = true
	}

	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}

	return result
}

// NewFieldError creates a new field error
func NewFieldError(field, message string, errorType ErrorTypes) *FieldError {
	return &FieldError{
		Field:   field,
		Message: message,
		Type:    errorType,
	}
}

// NewFieldErrorWithParam creates a new field error with a parameter
func NewFieldErrorWithParam(field, message, param string, errorType ErrorTypes) *FieldError {
	return &FieldError{
		Field:   field,
		Message: message,
		Type:    errorType,
		Param:   param,
	}
}

// FormatErrorMessage formats an error message
func FormatErrorMessage(field, rule string) string {
	switch rule {
	case "required":
		return fmt.Sprintf("%s is required", field)
	case "email":
		return fmt.Sprintf("%s must be a valid email address", field)
	case "url":
		return fmt.Sprintf("%s must be a valid URL", field)
	case "alpha":
		return fmt.Sprintf("%s must contain only letters", field)
	case "alphanum":
		return fmt.Sprintf("%s must contain only letters and numbers", field)
	case "numeric":
		return fmt.Sprintf("%s must be numeric", field)
	default:
		return fmt.Sprintf("%s validation failed for rule '%s'", field, rule)
	}
}

// FormatErrorMessageWithParam formats an error message with a parameter
func FormatErrorMessageWithParam(field, rule, param string) string {
	switch rule {
	case "min":
		return fmt.Sprintf("%s must be at least %s", field, param)
	case "max":
		return fmt.Sprintf("%s must be at most %s", field, param)
	case "len":
		return fmt.Sprintf("%s must be exactly %s characters", field, param)
	default:
		return fmt.Sprintf("%s validation failed for rule '%s' with parameter '%s'", field, rule, param)
	}
}
