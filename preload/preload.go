package preload

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/gomodul/db"
)

// Executor handles preloading of associations
type Executor struct {
	database *db.DB
}

// NewExecutor creates a new preload executor
func NewExecutor(database *db.DB) *Executor {
	return &Executor{database: database}
}

// Preload preloads associations for a single model
func (e *Executor) Preload(model interface{}, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}

	return e.PreloadModels([]interface{}{model}, fields...)
}

// PreloadModels preloads associations for multiple models
func (e *Executor) PreloadModels(models []interface{}, fields ...string) error {
	if len(models) == 0 || len(fields) == 0 {
		return nil
	}

	// Load each association
	for _, field := range fields {
		// Parse nested preloads (e.g., "Profile.User", "Posts.Comments")
		if strings.Contains(field, ".") {
			if err := e.preloadNested(models, field); err != nil {
				return fmt.Errorf("failed to preload nested association %s: %w", field, err)
			}
		} else {
			if err := e.preloadField(models, field); err != nil {
				return fmt.Errorf("failed to preload association %s: %w", field, err)
			}
		}
	}

	return nil
}

// preloadField preloads a single association field
func (e *Executor) preloadField(models []interface{}, field string) error {
	// Get associations for the first model
	associations := db.GetAssociations(models[0])
	assoc, exists := associations[field]
	if !exists {
		return fmt.Errorf("association %s not found", field)
	}

	config := assoc.Config

	switch config.Type {
	case db.AssociationHasOne:
		return e.preloadHasOne(models, field, config)
	case db.AssociationHasMany:
		return e.preloadHasMany(models, field, config)
	case db.AssociationBelongsTo:
		return e.preloadBelongsTo(models, field, config)
	default:
		return fmt.Errorf("unsupported association type: %s", config.Type)
	}
}

// preloadHasOne preloads has-one associations
func (e *Executor) preloadHasOne(models []interface{}, field string, config db.AssociationConfig) error {
	if config.Model == nil {
		return fmt.Errorf("model not defined for has-one association")
	}

	// Extract foreign keys
	fkMap := make(map[interface{}][]interface{})
	for _, model := range models {
		fkValue, err := db.ExtractForeignKey(model, config.ForeignKey)
		if err != nil {
			continue
		}
		if !isZeroValue(fkValue) {
			fkMap[fkValue] = append(fkMap[fkValue], model)
		}
	}

	if len(fkMap) == 0 {
		return nil
	}

	var keys []interface{}
	for k := range fkMap {
		keys = append(keys, k)
	}

	ref := config.References
	if ref == "" {
		ref = "id"
	}

	// Query using db.Model()
	relatedModels := reflect.New(reflect.SliceOf(reflect.TypeOf(config.Model))).Interface()
	qb := e.database.Model(config.Model)
	qb = qb.WhereIn(ref, keys...)

	if err := qb.Find(relatedModels); err != nil {
		return err
	}

	// Map results
	relatedMap := make(map[interface{}]interface{})
	relatedSlice := reflect.ValueOf(relatedModels).Elem()
	for i := 0; i < relatedSlice.Len(); i++ {
		related := relatedSlice.Index(i).Interface()
		refValue, _ := db.ExtractForeignKey(related, ref)
		relatedMap[refValue] = related
	}

	// Set values
	for fkValue, modelsToUpdate := range fkMap {
		if related, ok := relatedMap[fkValue]; ok {
			for _, model := range modelsToUpdate {
				setFieldValue(model, field, related)
			}
		}
	}

	return nil
}

// preloadHasMany preloads has-many associations
func (e *Executor) preloadHasMany(models []interface{}, field string, config db.AssociationConfig) error {
	if config.Model == nil {
		return fmt.Errorf("model not defined for has-many association")
	}

	// Extract reference values
	refMap := make(map[interface{}][]interface{})
	for _, model := range models {
		fkValue, err := db.ExtractForeignKey(model, config.References)
		if err != nil {
			continue
		}
		if !isZeroValue(fkValue) {
			refMap[fkValue] = append(refMap[fkValue], model)
		}
	}

	if len(refMap) == 0 {
		return nil
	}

	var keys []interface{}
	for k := range refMap {
		keys = append(keys, k)
	}

	fk := config.ForeignKey
	if fk == "" {
		return fmt.Errorf("foreign key not defined")
	}

	// Query using db.Model()
	relatedModels := reflect.New(reflect.SliceOf(reflect.TypeOf(config.Model))).Interface()
	qb := e.database.Model(config.Model)
	qb = qb.WhereIn(fk, keys...)

	if err := qb.Find(relatedModels); err != nil {
		return err
	}

	// Group results
	relatedMap := make(map[interface{}][]interface{})
	relatedSlice := reflect.ValueOf(relatedModels).Elem()
	for i := 0; i < relatedSlice.Len(); i++ {
		related := relatedSlice.Index(i).Interface()
		fkValue, _ := db.ExtractForeignKey(related, fk)
		relatedMap[fkValue] = append(relatedMap[fkValue], related)
	}

	// Set values
	for refValue, modelsToUpdate := range refMap {
		if related, ok := relatedMap[refValue]; ok {
			for _, model := range modelsToUpdate {
				setFieldValue(model, field, related)
			}
		}
	}

	return nil
}

// preloadBelongsTo preloads belongs-to associations
func (e *Executor) preloadBelongsTo(models []interface{}, field string, config db.AssociationConfig) error {
	if config.Model == nil {
		return fmt.Errorf("model not defined for belongs-to association")
	}

	// Extract foreign keys
	fkMap := make(map[interface{}][]interface{})
	for _, model := range models {
		fkValue, err := db.ExtractForeignKey(model, config.ForeignKey)
		if err != nil {
			continue
		}
		if !isZeroValue(fkValue) {
			fkMap[fkValue] = append(fkMap[fkValue], model)
		}
	}

	if len(fkMap) == 0 {
		return nil
	}

	var keys []interface{}
	for k := range fkMap {
		keys = append(keys, k)
	}

	ref := config.References
	if ref == "" {
		ref = "id"
	}

	// Query using db.Model()
	relatedModels := reflect.New(reflect.SliceOf(reflect.TypeOf(config.Model))).Interface()
	qb := e.database.Model(config.Model)
	qb = qb.WhereIn(ref, keys...)

	if err := qb.Find(relatedModels); err != nil {
		return err
	}

	// Map results
	relatedMap := make(map[interface{}]interface{})
	relatedSlice := reflect.ValueOf(relatedModels).Elem()
	for i := 0; i < relatedSlice.Len(); i++ {
		related := relatedSlice.Index(i).Interface()
		refValue, _ := db.ExtractForeignKey(related, ref)
		relatedMap[refValue] = related
	}

	// Set values
	for fkValue, modelsToUpdate := range fkMap {
		if related, ok := relatedMap[fkValue]; ok {
			for _, model := range modelsToUpdate {
				setFieldValue(model, field, related)
			}
		}
	}

	return nil
}

// preloadNested handles nested preloads
func (e *Executor) preloadNested(models []interface{}, field string) error {
	parts := strings.Split(field, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid nested preload: %s", field)
	}

	// Load first level
	if err := e.preloadField(models, parts[0]); err != nil {
		return err
	}

	// Collect loaded models
	var loadedModels []interface{}
	for _, model := range models {
		assocValue := getAssociationValue(model, parts[0])
		if assocValue == nil {
			continue
		}

		val := reflect.ValueOf(assocValue)
		if val.Kind() == reflect.Slice {
			for i := 0; i < val.Len(); i++ {
				loadedModels = append(loadedModels, val.Index(i).Interface())
			}
		} else if val.IsValid() && !val.IsNil() {
			loadedModels = append(loadedModels, assocValue)
		}
	}

	if len(loadedModels) == 0 {
		return nil
	}

	// Recursively preload remaining levels
	remaining := strings.Join(parts[1:], ".")
	return e.PreloadModels(loadedModels, remaining)
}

// getAssociationValue gets the value of an association field from a model
func getAssociationValue(model interface{}, fieldName string) interface{} {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return nil
	}

	field := rv.FieldByName(fieldName)
	if !field.IsValid() {
		field = findField(rv, fieldName)
	}

	if !field.IsValid() || !field.CanInterface() {
		return nil
	}

	return field.Interface()
}

// findField finds a field by name (case-insensitive)
func findField(rv reflect.Value, fieldName string) reflect.Value {
	rt := rv.Type()
	fieldNameLower := strings.ToLower(fieldName)

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if strings.ToLower(field.Name) == fieldNameLower {
			return rv.Field(i)
		}

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

// setFieldValue sets a field value on a model
func setFieldValue(model interface{}, fieldName string, value interface{}) error {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return fmt.Errorf("model is nil")
		}
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("model must be a struct")
	}

	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		if field.Name == fieldName {
			fieldVal := rv.Field(i)
			if !fieldVal.CanSet() {
				return fmt.Errorf("field %s cannot be set", fieldName)
			}

			valueRef := reflect.ValueOf(value)
			if valueRef.IsValid() {
				if fieldVal.Kind() == reflect.Ptr {
					if valueRef.Kind() == reflect.Ptr {
						fieldVal.Set(valueRef)
					} else {
						ptr := reflect.New(valueRef.Type())
						ptr.Elem().Set(valueRef)
						fieldVal.Set(ptr)
					}
				} else {
					fieldVal.Set(valueRef)
				}
			}
			return nil
		}

		if tag := field.Tag.Get("db"); tag != "" {
			if idx := strings.Index(tag, ","); idx != -1 {
				if tag[:idx] == fieldName {
					fieldVal := rv.Field(i)
					if !fieldVal.CanSet() {
						return fmt.Errorf("field %s cannot be set", fieldName)
					}

					valueRef := reflect.ValueOf(value)
					if valueRef.IsValid() {
						if fieldVal.Kind() == reflect.Ptr {
							if valueRef.Kind() == reflect.Ptr {
								fieldVal.Set(valueRef)
							} else {
								ptr := reflect.New(valueRef.Type())
								ptr.Elem().Set(valueRef)
								fieldVal.Set(ptr)
							}
						} else {
							fieldVal.Set(valueRef)
						}
					}
					return nil
				}
			}
		}
	}

	return fmt.Errorf("field %s not found", fieldName)
}

// isZeroValue checks if a value is zero
func isZeroValue(value interface{}) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
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
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() == 0
	default:
		return reflect.DeepEqual(rv.Interface(), reflect.Zero(rv.Type()).Interface())
	}
}

// HasPreload checks if a model has any preloads configured
func HasPreload(model interface{}) bool {
	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return false
	}

	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("preload")

		if tag != "" && tag != "-" {
			return true
		}
	}

	return false
}

// GetPreloads gets all preloads configured for a model
func GetPreloads(model interface{}) []string {
	var preloads []string

	rv := reflect.ValueOf(model)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return preloads
	}

	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("preload")

		if tag != "" && tag != "-" {
			parts := strings.Split(tag, ",")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part != "" {
					preloads = append(preloads, part)
				}
			}
		}
	}

	return preloads
}
