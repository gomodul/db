package validation

import (
	"reflect"
	"strings"
)

// TagParser parses struct tags for validation rules
type TagParser struct {
	tagName string
}

// NewTagParser creates a new tag parser
func NewTagParser(tagName string) *TagParser {
	return &TagParser{
		tagName: tagName,
	}
}

// Parse parses validation tags from a struct
func (p *TagParser) Parse(model interface{}) []FieldValidation {
	var validations []FieldValidation

	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return validations
	}

	rt := rv.Type()

	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i)
		structField := rt.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		// Get validation tags
		tag := structField.Tag.Get(p.tagName)
		if tag == "" || tag == "-" {
			continue
		}

		validations = append(validations, FieldValidation{
			Field:    structField.Name,
			Value:    field.Interface(),
			Rules:    parseValidationTag(tag),
			JSONName: getJSONName(structField),
		})
	}

	return validations
}

// FieldValidation represents a field validation
type FieldValidation struct {
	Field    string
	JSONName string
	Value    interface{}
	Rules    []ValidationRuleDef
}

// getJSONName gets the JSON name from a struct field
func getJSONName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag != "" && tag != "-" {
		if idx := strings.Index(tag, ","); idx != -1 {
			name := tag[:idx]
			if name != "" {
				return name
			}
		} else {
			return tag
		}
	}
	return field.Name
}

// ValidateTag validates a value based on a tag string
func ValidateTag(value interface{}, tag string) error {
	if tag == "" || tag == "-" {
		return nil
	}

	rules := parseValidationTag(tag)
	v := NewValidator()

	for _, rule := range rules {
		if rule.Param != "" {
			if err := executeParametrizedRule(rule.Name, rule.Param, value); err != nil {
				return err
			}
		} else {
			ruleFunc, exists := v.rules[rule.Name]
			if !exists {
				continue // Skip unknown rules
			}
			if err := ruleFunc(value); err != nil {
				return err
			}
		}
	}

	return nil
}

// ValidateStruct validates a struct based on its tags
func ValidateStruct(model interface{}) error {
	v := NewValidator()
	return v.Validate(model)
}

// ValidateField validates a single field
func ValidateField(field interface{}, tag string) error {
	return ValidateTag(field, tag)
}
