package associations

import (
	"fmt"
	"reflect"
	"strings"
)

// PolymorphicType represents the type of polymorphic association
type PolymorphicType string

const (
	PolymorphicHasOne  PolymorphicType = "polymorphic_has_one"
	PolymorphicHasMany PolymorphicType = "polymorphic_has_many"
)

// PolymorphicAssociation represents a polymorphic association
type PolymorphicAssociation struct {
	name           string
	polymorphicType PolymorphicType
	typeField      string
	idField        string
	models         map[string]any // type -> model mapping
}

// NewPolymorphicAssociation creates a new polymorphic association
func NewPolymorphicAssociation(name string, pType PolymorphicType, typeField, idField string) *PolymorphicAssociation {
	return &PolymorphicAssociation{
		name:           name,
		polymorphicType: pType,
		typeField:      typeField,
		idField:        idField,
		models:         make(map[string]any),
	}
}

// RegisterModel registers a model for a polymorphic type
func (pa *PolymorphicAssociation) RegisterModel(typeValue string, model any) {
	pa.models[typeValue] = model
}

// GetModel returns the model for a given type
func (pa *PolymorphicAssociation) GetModel(typeValue string) (any, bool) {
	model, ok := pa.models[typeValue]
	return model, ok
}

// Name returns the association name
func (pa *PolymorphicAssociation) Name() string {
	return pa.name
}

// TypeField returns the type field name
func (pa *PolymorphicAssociation) TypeField() string {
	return pa.typeField
}

// IDField returns the ID field name
func (pa *PolymorphicAssociation) IDField() string {
	return pa.idField
}

// IsPolymorphic returns true if this is a polymorphic association
func (pa *PolymorphicAssociation) IsPolymorphic() bool {
	return true
}

// PolymorphicValue represents a polymorphic value
type PolymorphicValue struct {
	Type string
	ID   any
}

// GetPolymorphicValue extracts the polymorphic value from a model
func GetPolymorphicValue(model any, typeField, idField string) (*PolymorphicValue, error) {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct")
	}

	rt := rv.Type()

	var typeValue string
	var idValue any

	// Find type field
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		// Check by field name
		if strings.EqualFold(field.Name, typeField) {
			typeValue = strings.ToLower(rv.Field(i).String())
			break
		}

		// Check tags
		if tag := field.Tag.Get("json"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], typeField) {
					typeValue = strings.ToLower(rv.Field(i).String())
					break
				}
			} else if strings.EqualFold(tag, typeField) {
				typeValue = strings.ToLower(rv.Field(i).String())
				break
			}
		}

		if tag := field.Tag.Get("db"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], typeField) {
					typeValue = strings.ToLower(rv.Field(i).String())
					break
				}
			} else if strings.EqualFold(tag, typeField) {
				typeValue = strings.ToLower(rv.Field(i).String())
				break
			}
		}
	}

	// Find ID field
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		// Check by field name
		if strings.EqualFold(field.Name, idField) {
			idValue = rv.Field(i).Interface()
			break
		}

		// Check tags
		if tag := field.Tag.Get("json"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], idField) {
					idValue = rv.Field(i).Interface()
					break
				}
			} else if strings.EqualFold(tag, idField) {
				idValue = rv.Field(i).Interface()
				break
			}
		}

		if tag := field.Tag.Get("db"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], idField) {
					idValue = rv.Field(i).Interface()
					break
				}
			} else if strings.EqualFold(tag, idField) {
				idValue = rv.Field(i).Interface()
				break
			}
		}
	}

	if typeValue == "" {
		return nil, fmt.Errorf("type field %s not found", typeField)
	}

	if idValue == nil {
		return nil, fmt.Errorf("ID field %s not found", idField)
	}

	return &PolymorphicValue{
		Type: typeValue,
		ID:   idValue,
	}, nil
}

// SetPolymorphicValue sets polymorphic values on a model
func SetPolymorphicValue(model any, typeField, idField string, typeValue string, idValue any) error {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return fmt.Errorf("cannot set value on nil pointer")
		}
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct")
	}

	rt := rv.Type()

	// Set type field
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		fieldName := field.Name
		if tag := field.Tag.Get("json"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], typeField) {
					fieldName = tag[:idx]
				}
			} else if strings.EqualFold(tag, typeField) {
				fieldName = tag
			}
		} else if tag := field.Tag.Get("db"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], typeField) {
					fieldName = tag[:idx]
				}
			} else if strings.EqualFold(tag, typeField) {
				fieldName = tag
			}
		}

		if strings.EqualFold(fieldName, typeField) || strings.EqualFold(field.Name, typeField) {
			fieldVal := rv.Field(i)
			if fieldVal.CanSet() {
				if fieldVal.Kind() == reflect.String {
					fieldVal.SetString(typeValue)
				}
			}
			break
		}
	}

	// Set ID field
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		fieldName := field.Name
		if tag := field.Tag.Get("json"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], idField) {
					fieldName = tag[:idx]
				}
			} else if strings.EqualFold(tag, idField) {
				fieldName = tag
			}
		} else if tag := field.Tag.Get("db"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.EqualFold(tag[:idx], idField) {
					fieldName = tag[:idx]
				}
			} else if strings.EqualFold(tag, idField) {
				fieldName = tag
			}
		}

		if strings.EqualFold(fieldName, idField) || strings.EqualFold(field.Name, idField) {
			fieldVal := rv.Field(i)
			if fieldVal.CanSet() {
				val := reflect.ValueOf(idValue)
				if val.Type().ConvertibleTo(fieldVal.Type()) {
					fieldVal.Set(val.Convert(fieldVal.Type()))
				}
			}
			break
		}
	}

	return nil
}

// PolymorphicRegistry manages polymorphic associations
type PolymorphicRegistry struct {
	associations map[string]*PolymorphicAssociation
	modelTypes   map[string]string // model name -> type mapping
}

// NewPolymorphicRegistry creates a new polymorphic registry
func NewPolymorphicRegistry() *PolymorphicRegistry {
	return &PolymorphicRegistry{
		associations: make(map[string]*PolymorphicAssociation),
		modelTypes:   make(map[string]string),
	}
}

// Register registers a polymorphic association
func (pr *PolymorphicRegistry) Register(assoc *PolymorphicAssociation) {
	pr.associations[assoc.name] = assoc
}

// RegisterModelType registers a model type
func (pr *PolymorphicRegistry) RegisterModelType(modelName, typeValue string) {
	pr.modelTypes[modelName] = typeValue
}

// GetAssociation returns a polymorphic association by name
func (pr *PolymorphicRegistry) GetAssociation(name string) (*PolymorphicAssociation, bool) {
	assoc, ok := pr.associations[name]
	return assoc, ok
}

// GetTypeForModel returns the type value for a model
func (pr *PolymorphicRegistry) GetTypeForModel(modelName string) (string, bool) {
	typeValue, ok := pr.modelTypes[modelName]
	return typeValue, ok
}

// IsPolymorphicField checks if a field is a polymorphic association
func IsPolymorphicField(field reflect.StructField) bool {
	tag := field.Tag.Get("db")
	return strings.Contains(tag, ",polymorphic")
}

// ParsePolymorphicTag parses a polymorphic tag
//
// Tag format: "polymorphic:type_field:id_field"
// Example: `db:"imageablepolymorphic:imageable_type:imageable_id"`
func ParsePolymorphicTag(tag string) (string, string, bool) {
	if !strings.Contains(tag, ",polymorphic:") {
		return "", "", false
	}

	// Get the part after "polymorphic:"
	afterPolymorphic := strings.Split(tag, "polymorphic:")[1]
	parts := strings.Split(afterPolymorphic, ",")

	if len(parts) == 0 {
		return "", "", false
	}

	config := strings.TrimSpace(parts[0])
	fields := strings.Split(config, ":")

	if len(fields) >= 2 {
		return fields[0], fields[1], true
	}

	return "", "", false
}
