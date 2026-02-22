package db

import (
	"fmt"
	"reflect"
	"strings"
)

// AssociationType represents the type of association.
type AssociationType string

const (
	AssociationHasOne    AssociationType = "has_one"
	AssociationHasMany   AssociationType = "has_many"
	AssociationBelongsTo AssociationType = "belongs_to"
)

// AssociationConfig holds association configuration.
type AssociationConfig struct {
	Type        AssociationType
	ForeignKey  string
	References  string
	Model       any
	Joins       string // Custom JOIN clause for HasMany
}

// Association represents a relationship between models.
type Association struct {
	name   string
	Config AssociationConfig
}

// Name returns the association name
func (a *Association) Name() string {
	return a.name
}

// Type returns the association type as a string
func (a *Association) Type() AssociationType {
	return a.Config.Type
}

// Config returns the association configuration
func (a *Association) GetConfig() *AssociationConfig {
	return &a.Config
}

// GetForeignKey returns the foreign key
func (a *Association) GetForeignKey() string {
	return a.Config.ForeignKey
}

// GetReferences returns the references field
func (a *Association) GetReferences() string {
	return a.Config.References
}

// GetModel returns the associated model
func (a *Association) GetModel() any {
	return a.Config.Model
}

// HasOne defines a has-one relationship.
//
//	type User struct {
//	    ID       int64
//	    Profile  *Profile `db:"profile_id,pk"`
//	}
//
// The Profile field will be automatically loaded when using Preload.
func HasOne(model any, foreignKey, references string) *Association {
	return &Association{
		Config: AssociationConfig{
			Type:       AssociationHasOne,
			ForeignKey: foreignKey,
			References: references,
			Model:      model,
		},
	}
}

// HasMany defines a has-many relationship.
//
//	type User struct {
//	    ID    int64
//	    Posts []Post `db:"posts"`
//	}
func HasMany(model any, foreignKey, references string) *Association {
	return &Association{
		Config: AssociationConfig{
			Type:       AssociationHasMany,
			ForeignKey: foreignKey,
			References: references,
			Model:      model,
		},
	}
}

// BelongsTo defines a belongs-to relationship.
//
//	type Post struct {
//	    ID     int64
//	    UserID int64
//	    User   User `db:"user_id"`
//	}
func BelongsTo(model any, foreignKey, references string) *Association {
	return &Association{
		Config: AssociationConfig{
			Type:       AssociationBelongsTo,
			ForeignKey: foreignKey,
			References: references,
			Model:      model,
		},
	}
}

// GetAssociations extracts association definitions from a model struct.
// It looks for fields with association tags and returns the configured associations.
func GetAssociations(model any) map[string]*Association {
	associations := make(map[string]*Association)

	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return associations
	}

	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag := f.Tag.Get("db")

		// Check for association tags
		if strings.Contains(tag, ",has_one") {
			name := strings.ToLower(f.Name)
			associations[name] = &Association{
				name: name,
				Config: AssociationConfig{
					Type: AssociationHasOne,
				},
			}
		} else if strings.Contains(tag, ",has_many") {
			name := strings.ToLower(f.Name)
			associations[name] = &Association{
				name: name,
				Config: AssociationConfig{
					Type: AssociationHasMany,
				},
			}
		} else if strings.Contains(tag, ",belongs_to") {
			name := strings.ToLower(f.Name)
			associations[name] = &Association{
				name: name,
				Config: AssociationConfig{
					Type: AssociationBelongsTo,
				},
			}
		}
	}

	return associations
}

// ExtractForeignKey extracts the foreign key value from a model for an association.
func ExtractForeignKey(model any, foreignKey string) (any, error) {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model must be a struct")
	}

	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag := f.Tag.Get("db")

		// Extract column name from tag
		name := tag
		if comma := strings.Index(tag, ","); comma != -1 {
			name = tag[:comma]
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}

		if name == foreignKey {
			return rv.Field(i).Interface(), nil
		}
	}

	return nil, fmt.Errorf("foreign key %s not found in model", foreignKey)
}

// GetAssociationTableName returns the table name for an association model.
func GetAssociationTableName(association *Association) string {
	// If the association has a model, extract table name from it
	if association.Config.Model != nil {
		// Use reflection to get the table name
		rv := reflect.ValueOf(association.Config.Model)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		if rv.Kind() == reflect.Struct {
			rt := rv.Type()

			// Check for table tag
			if field, ok := rt.FieldByName("_"); ok {
				if tag := field.Tag.Get("table"); tag != "" {
					return tag
				}
			}

			// Default to struct name in lowercase + "s"
			name := strings.ToLower(rt.Name())
			if !strings.HasSuffix(name, "s") {
				name += "s"
			}
			return name
		}
	}

	return ""
}
