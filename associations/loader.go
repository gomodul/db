package associations

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/gomodul/db"
	"github.com/gomodul/db/builder"
)

// Type represents the association type (re-export for convenience)
type Type = string

const (
	HasOne    Type = "has_one"
	HasMany   Type = "has_many"
	BelongsTo Type = "belongs_to"
	Many2Many Type = "many2many"
)

// Loader handles loading associations for models
type Loader struct {
	db interface{} // Can be *db.DB or *builder.DB
	mu sync.RWMutex
}

// NewLoader creates a new association loader
func NewLoader(database interface{}) *Loader {
	return &Loader{
		db: database,
	}
}

// Load loads associations for a model
func (l *Loader) Load(model any, associations ...string) error {
	return l.LoadModels([]any{model}, associations...)
}

// LoadModels loads associations for multiple models
func (l *Loader) LoadModels(models []any, associations ...string) error {
	if len(models) == 0 {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Load each association
	for _, assocName := range associations {
		// Get association info from the first model
		assoc := l.getAssociationInfo(models[0], assocName)
		if assoc == nil {
			continue // Skip unknown associations
		}

		// Load based on association type
		if err := l.loadAssociation(models, assocName, assoc); err != nil {
			return fmt.Errorf("failed to load association %s: %w", assocName, err)
		}
	}

	return nil
}

// loadAssociation loads a single association for models
func (l *Loader) loadAssociation(models []any, assocName string, assoc *db.Association) error {
	assocType := db.GetAssociations(models[0])[assocName].Config.Type

	switch db.AssociationType(assocType) {
	case db.AssociationHasOne:
		return l.loadHasOne(models, assocName, assoc)
	case db.AssociationHasMany:
		return l.loadHasMany(models, assocName, assoc)
	case db.AssociationBelongsTo:
		return l.loadBelongsTo(models, assocName, assoc)
	default:
		return fmt.Errorf("unknown association type: %s", assocType)
	}
}

// loadHasOne loads has-one associations
func (l *Loader) loadHasOne(models []any, assocName string, assoc *db.Association) error {
	// Get association config
	config := assoc.Config

	// Extract foreign keys from models
	fkValues := make(map[any][]any) // fk value -> models

	for _, model := range models {
		fkValue, err := db.ExtractForeignKey(model, config.ForeignKey)
		if err != nil {
			continue
		}

		// Skip zero values
		if isZeroValue(reflect.ValueOf(fkValue)) {
			continue
		}

		fkValues[fkValue] = append(fkValues[fkValue], model)
	}

	// Query related models
	var fkKeys []any
	for key := range fkValues {
		fkKeys = append(fkKeys, key)
	}

	if len(fkKeys) == 0 {
		return nil
	}

	// Get the reference field name (usually "id")
	ref := config.References
	if ref == "" {
		ref = "id"
	}

	// Build query to get related models
	relatedModels := reflect.New(reflect.SliceOf(reflect.TypeOf(config.Model))).Interface()

	// Use db.Model() to query - l.db must be *db.DB
	dbase, ok := l.db.(*db.DB)
	if !ok {
		return fmt.Errorf("loader requires *db.DB")
	}

	qb := dbase.Model(config.Model)
	qb = qb.WhereIn(ref, fkKeys...)
	if err := qb.Find(relatedModels); err != nil {
		return err
	}

	// Map related models by their reference value
	relatedMap := make(map[any]any)
	relatedSlice := reflect.ValueOf(relatedModels).Elem()

	for i := 0; i < relatedSlice.Len(); i++ {
		related := relatedSlice.Index(i).Interface()
		refValue, err := db.ExtractForeignKey(related, ref)
		if err != nil {
			continue
		}
		relatedMap[refValue] = related
	}

	// Set the associated models
	for fkValue, modelsToUpdate := range fkValues {
		if related, ok := relatedMap[fkValue]; ok {
			for _, model := range modelsToUpdate {
				if err := setAssociationField(model, assocName, related); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// loadHasMany loads has-many associations
func (l *Loader) loadHasMany(models []any, assocName string, assoc *db.Association) error {
	// Get association config
	config := assoc.Config

	// Extract reference values from models
	refValues := make(map[any][]any) // ref value -> models

	for _, model := range models {
		fkValue, err := db.ExtractForeignKey(model, config.References)
		if err != nil {
			continue
		}

		// Skip zero values
		if isZeroValue(reflect.ValueOf(fkValue)) {
			continue
		}

		refValues[fkValue] = append(refValues[fkValue], model)
	}

	// Query related models
	var refKeys []any
	for key := range refValues {
		refKeys = append(refKeys, key)
	}

	if len(refKeys) == 0 {
		return nil
	}

	// Get the foreign key field name
	fk := config.ForeignKey
	if fk == "" {
		return fmt.Errorf("foreign key not defined for has-many association")
	}

	// Build query to get related models
	relatedModels := reflect.New(reflect.SliceOf(reflect.TypeOf(config.Model))).Interface()

	// Get query builder - handle both *db.DB and *builder.DB
	var qb *builder.QueryBuilder
	if dbase, ok := l.db.(*db.DB); ok {
		qb = dbase.Model(config.Model)
	} else {
		return fmt.Errorf("loader requires *db.DB")
	}
	qb = qb.WhereIn(fk, refKeys...)

	if err := qb.Find(relatedModels); err != nil {
		return err
	}

	// Group related models by their foreign key value
	relatedMap := make(map[any][]any)
	relatedSlice := reflect.ValueOf(relatedModels).Elem()

	for i := 0; i < relatedSlice.Len(); i++ {
		related := relatedSlice.Index(i).Interface()
		fkValue, err := db.ExtractForeignKey(related, fk)
		if err != nil {
			continue
		}
		relatedMap[fkValue] = append(relatedMap[fkValue], related)
	}

	// Set the associated models
	for refValue, modelsToUpdate := range refValues {
		if related, ok := relatedMap[refValue]; ok {
			for _, model := range modelsToUpdate {
				if err := setAssociationField(model, assocName, related); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// loadBelongsTo loads belongs-to associations
func (l *Loader) loadBelongsTo(models []any, assocName string, assoc *db.Association) error {
	// Get association config
	config := assoc.Config

	// Extract foreign keys from models
	fkValues := make(map[any][]any) // fk value -> models

	for _, model := range models {
		fkValue, err := db.ExtractForeignKey(model, config.ForeignKey)
		if err != nil {
			continue
		}

		// Skip zero values
		if isZeroValue(reflect.ValueOf(fkValue)) {
			continue
		}

		fkValues[fkValue] = append(fkValues[fkValue], model)
	}

	// Query related models
	var fkKeys []any
	for key := range fkValues {
		fkKeys = append(fkKeys, key)
	}

	if len(fkKeys) == 0 {
		return nil
	}

	// Get the reference field name (usually "id")
	ref := config.References
	if ref == "" {
		ref = "id"
	}

	// Build query to get related models
	relatedModels := reflect.New(reflect.SliceOf(reflect.TypeOf(config.Model))).Interface()

	// Get query builder - handle both *db.DB and *builder.DB
	var qb *builder.QueryBuilder
	if dbase, ok := l.db.(*db.DB); ok {
		qb = dbase.Model(config.Model)
	} else {
		return fmt.Errorf("loader requires *db.DB")
	}
	qb = qb.WhereIn(ref, fkKeys...)

	if err := qb.Find(relatedModels); err != nil {
		return err
	}

	// Map related models by their reference value
	relatedMap := make(map[any]any)
	relatedSlice := reflect.ValueOf(relatedModels).Elem()

	for i := 0; i < relatedSlice.Len(); i++ {
		related := relatedSlice.Index(i).Interface()
		refValue, err := db.ExtractForeignKey(related, ref)
		if err != nil {
			continue
		}
		relatedMap[refValue] = related
	}

	// Set the associated models
	for fkValue, modelsToUpdate := range fkValues {
		if related, ok := relatedMap[fkValue]; ok {
			for _, model := range modelsToUpdate {
				if err := setAssociationField(model, assocName, related); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// getAssociationInfo gets association info from a model
func (l *Loader) getAssociationInfo(model any, assocName string) *db.Association {
	associations := db.GetAssociations(model)
	assoc, ok := associations[assocName]
	if ok {
		return assoc
	}
	return nil
}

// setAssociationField sets an association field on a model
func setAssociationField(model any, fieldName string, value any) error {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct")
	}

	field := rv.FieldByName(fieldName)
	if !field.IsValid() {
		// Try case-insensitive search
		field = findFieldByName(rv, fieldName)
		if field == (reflect.Value{}) {
			return fmt.Errorf("field %s not found in model", fieldName)
		}
	}

	// Check if field is settable
	if !field.CanSet() {
		return fmt.Errorf("field %s cannot be set", fieldName)
	}

	valueRef := reflect.ValueOf(value)

	// Handle different field types
	if field.Kind() == reflect.Ptr {
		if valueRef.Kind() == reflect.Slice {
			// Field is pointer to slice, value is slice
			field.Set(valueRef)
		} else if valueRef.Kind() == reflect.Ptr {
			field.Set(valueRef)
		} else {
			// Create pointer to value
			ptr := reflect.New(valueRef.Type())
			ptr.Elem().Set(valueRef)
			field.Set(ptr)
		}
	} else if field.Kind() == reflect.Slice {
		if valueRef.Kind() == reflect.Slice {
			field.Set(valueRef)
		} else {
			// Wrap single value in slice
			sliceType := field.Type()
			slice := reflect.MakeSlice(sliceType, 1, 1)
			slice.Index(0).Set(valueRef)
			field.Set(slice)
		}
	} else {
		field.Set(valueRef)
	}

	return nil
}

// findFieldByName finds a field by name (case-insensitive)
func findFieldByName(rv reflect.Value, fieldName string) reflect.Value {
	rt := rv.Type()
	fieldNameLower := strings.ToLower(fieldName)

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if strings.ToLower(field.Name) == fieldNameLower {
			return rv.Field(i)
		}

		// Check tags
		if tag := field.Tag.Get("json"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.ToLower(tag[:idx]) == fieldNameLower {
					return rv.Field(i)
				}
			} else if strings.ToLower(tag) == fieldNameLower {
				return rv.Field(i)
			}
		}

		if tag := field.Tag.Get("db"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if strings.ToLower(tag[:idx]) == fieldNameLower {
					return rv.Field(i)
				}
			} else if strings.ToLower(tag) == fieldNameLower {
				return rv.Field(i)
			}
		}
	}

	return reflect.Value{}
}

// getModelName gets the model name
func getModelName(model any) string {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return ""
	}

	rt := rv.Type()
	return rt.Name()
}

// isZeroValue checks if a value is zero
func isZeroValue(rv reflect.Value) bool {
	switch rv.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Ptr, reflect.Interface:
		return rv.IsNil()
	case reflect.Bool:
		return !rv.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return rv.Float() == 0
	case reflect.String:
		return rv.String() == ""
	case reflect.Slice, reflect.Array:
		return rv.Len() == 0
	case reflect.Map:
		return rv.Len() == 0
	default:
		return reflect.DeepEqual(rv.Interface(), reflect.Zero(rv.Type()).Interface())
	}
}
