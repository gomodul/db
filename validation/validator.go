package validation

import (
	"fmt"
	"reflect"
	"strings"
)

// Validator is the interface for validation
type Validator interface {
	Validate(model interface{}) error
	ValidateField(field interface{}, tag string) error
}

// ValidatorImpl is the default validator implementation
type ValidatorImpl struct {
	rules map[string]ValidationRule
}

// ValidationRule is a function that validates a value
type ValidationRule func(interface{}) error

// NewValidator creates a new validator
func NewValidator() *ValidatorImpl {
	v := &ValidatorImpl{
		rules: make(map[string]ValidationRule),
	}

	// Register built-in rules
	v.RegisterRule("required", Required)
	v.RegisterRule("email", Email)
	v.RegisterRule("url", URL)
	v.RegisterRule("min", Min)
	v.RegisterRule("max", Max)
	v.RegisterRule("len", Length)
	v.RegisterRule("alpha", Alpha)
	v.RegisterRule("alphanum", Alphanumeric)
	v.RegisterRule("numeric", Numeric)

	return v
}

// RegisterRule registers a new validation rule
func (v *ValidatorImpl) RegisterRule(name string, rule ValidationRule) {
	v.rules[name] = rule
}

// Validate validates a model based on struct tags
func (v *ValidatorImpl) Validate(model interface{}) error {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct or pointer to struct")
	}

	rt := rv.Type()
	var errors []string

	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i)
		structField := rt.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		// Get validation tags
		tag := structField.Tag.Get("validate")
		if tag == "" || tag == "-" {
			continue
		}

		// Parse validation tags
		rules := parseValidationTag(tag)

		// Validate each rule
		for _, rule := range rules {
			if err := v.validateField(field, rule); err != nil {
				fieldName := getFieldName(structField)
				errors = append(errors, fmt.Sprintf("%s: %s", fieldName, err.Error()))
			}
		}
	}

	if len(errors) > 0 {
		return &ValidationError{
			Errors: errors,
		}
	}

	return nil
}

// ValidateField validates a single field
func (v *ValidatorImpl) ValidateField(field interface{}, tag string) error {
	rules := parseValidationTag(tag)
	for _, rule := range rules {
		if err := v.validateField(reflect.ValueOf(field), rule); err != nil {
			return err
		}
	}
	return nil
}

// validateField validates a field with a specific rule
func (v *ValidatorImpl) validateField(field reflect.Value, rule ValidationRuleDef) error {
	// Get the actual value
	value := field.Interface()
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			value = nil
		} else {
			value = field.Elem().Interface()
		}
	}

	// Get the rule function
	ruleFunc, exists := v.rules[rule.Name]
	if !exists {
		return fmt.Errorf("unknown validation rule: %s", rule.Name)
	}

	// Set up context for parameterized rules
	if rule.Param != "" {
		// For rules with parameters, we need special handling
		return executeParametrizedRule(rule.Name, rule.Param, value)
	}

	return ruleFunc(value)
}

// ValidationRuleDef defines a validation rule with optional parameters
type ValidationRuleDef struct {
	Name  string
	Param string
}

// parseValidationTag parses a validation tag into rule definitions
func parseValidationTag(tag string) []ValidationRuleDef {
	var rules []ValidationRuleDef

	parts := strings.Split(tag, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Check for parameterized rules (e.g., "min:5", "max:100")
		if idx := strings.Index(part, ":"); idx != -1 {
			rules = append(rules, ValidationRuleDef{
				Name:  part[:idx],
				Param: part[idx+1:],
			})
		} else {
			rules = append(rules, ValidationRuleDef{
				Name: part,
			})
		}
	}

	return rules
}

// getFieldName gets the field name from struct field
func getFieldName(field reflect.StructField) string {
	// Check for json tag
	if tag := field.Tag.Get("json"); tag != "" && tag != "-" {
		if idx := strings.Index(tag, ","); idx != -1 {
			name := tag[:idx]
			if name != "" {
				return name
			}
		} else {
			return tag
		}
	}

	// Check for db tag
	if tag := field.Tag.Get("db"); tag != "" && tag != "-" {
		if idx := strings.Index(tag, ","); idx != -1 {
			name := tag[:idx]
			if name != "" {
				return name
			}
		} else {
			return tag
		}
	}

	// Default to field name
	return strings.ToLower(field.Name)
}

// executeParametrizedRule executes a validation rule with parameters
func executeParametrizedRule(name, param string, value interface{}) error {
	switch name {
	case "min":
		return MinWithParam(value, param)
	case "max":
		return MaxWithParam(value, param)
	case "len":
		return LengthWithParam(value, param)
	default:
		return fmt.Errorf("unknown validation rule: %s", name)
	}
}

// ValidationError represents validation errors
type ValidationError struct {
	Errors []string
}

// Error implements the error interface
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %s", strings.Join(e.Errors, "; "))
}

// HasErrors returns true if there are validation errors
func (e *ValidationError) HasErrors() bool {
	return len(e.Errors) > 0
}

// All returns all validation errors
func (e *ValidationError) All() []string {
	return e.Errors
}
