package builder

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrNotFound is returned when no record is found
	ErrNotFound = errors.New("record not found")

	// ErrNotSupported is returned when an operation is not supported
	ErrNotSupported = errors.New("operation not supported")
)

// ============ Additional Scan Methods ============

// ScanRow scans a single row into dest
func (b *QueryBuilder) ScanRow(dest interface{}) error {
	return b.First(dest)
}

// ScanRows scans multiple rows into dest
func (b *QueryBuilder) ScanRows(dest interface{}) error {
	return b.Find(dest)
}

// ScanOne scans the first result or returns ErrNotFound
func (b *QueryBuilder) ScanOne(dest interface{}) error {
	err := b.First(dest)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// Rows returns a Rows iterator
func (b *QueryBuilder) Rows() (*Rows, error) {
	result, err := b.execute()
	if err != nil {
		return nil, err
	}

	return &Rows{
		data:  result.Data,
		index: -1,
	}, nil
}

// ScanEach iterates over results and calls fn for each
func (b *QueryBuilder) ScanEach(fn interface{}) error {
	result, err := b.execute()
	if err != nil {
		return err
	}

	fnVal := reflect.ValueOf(fn)
	fnType := fnVal.Type()

	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("ScanEach requires a function")
	}

	for _, item := range result.Data {
		args := []reflect.Value{reflect.ValueOf(item)}
		results := fnVal.Call(args)

		if len(results) > 0 {
			if err, ok := results[0].Interface().(error); ok && err != nil {
				return err
			}
		}
	}

	return nil
}

// ScanMap scans results into a slice of maps
func (b *QueryBuilder) ScanMap() ([]map[string]interface{}, error) {
	result, err := b.execute()
	if err != nil {
		return nil, err
	}

	maps := make([]map[string]interface{}, 0, len(result.Data))
	for _, item := range result.Data {
		if m, ok := item.(map[string]interface{}); ok {
			maps = append(maps, m)
		} else {
			// Try to convert struct to map
			m, err := b.structToMap(item)
			if err == nil {
				maps = append(maps, m)
			}
		}
	}

	return maps, nil
}

// structToMap converts a struct to map[string]interface{}
func (b *QueryBuilder) structToMap(obj interface{}) (map[string]interface{}, error) {
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %T", obj)
	}

	result := make(map[string]interface{})
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		name := getFieldName(field)
		if name == "-" {
			continue
		}

		result[name] = fieldVal.Interface()
	}

	return result, nil
}

// Rows represents an iterator over query results
type Rows struct {
	data  []interface{}
	index int
}

// Next advances to the next row
func (r *Rows) Next() bool {
	r.index++
	return r.index < len(r.data)
}

// Scan copies the current row into dest.
// dest must be a pointer to a struct, map, or the same concrete type returned by the driver.
func (r *Rows) Scan(dest interface{}) error {
	if r.index < 0 || r.index >= len(r.data) {
		return ErrNotFound
	}
	src := r.data[r.index]
	return scanInto(dest, src)
}

// scanInto copies src into dest using reflection.
func scanInto(dest, src interface{}) error {
	if dest == nil {
		return fmt.Errorf("scan: dest must not be nil")
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.IsNil() {
		return fmt.Errorf("scan: dest must be a non-nil pointer")
	}
	destElem := destVal.Elem()

	srcVal := reflect.ValueOf(src)
	if srcVal.Kind() == reflect.Ptr {
		srcVal = srcVal.Elem()
	}

	// Direct assignment if types match
	if srcVal.Type().AssignableTo(destElem.Type()) {
		destElem.Set(srcVal)
		return nil
	}

	// map[string]interface{} → struct
	if srcVal.Kind() == reflect.Map && destElem.Kind() == reflect.Struct {
		return mapToStruct(srcVal, destElem)
	}

	// struct → struct (field-by-field by name)
	if srcVal.Kind() == reflect.Struct && destElem.Kind() == reflect.Struct {
		return structToStruct(srcVal, destElem)
	}

	return fmt.Errorf("scan: cannot scan %T into %T", src, dest)
}

func mapToStruct(m, s reflect.Value) error {
	t := s.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := getFieldName(field)
		if name == "-" {
			continue
		}
		val := m.MapIndex(reflect.ValueOf(name))
		if !val.IsValid() {
			continue
		}
		fv := s.Field(i)
		v := reflect.ValueOf(val.Interface())
		if v.Type().ConvertibleTo(fv.Type()) {
			fv.Set(v.Convert(fv.Type()))
		}
	}
	return nil
}

func structToStruct(src, dst reflect.Value) error {
	dstType := dst.Type()
	for i := 0; i < dstType.NumField(); i++ {
		field := dstType.Field(i)
		if !field.IsExported() {
			continue
		}
		srcField := src.FieldByName(field.Name)
		if !srcField.IsValid() {
			continue
		}
		dstField := dst.Field(i)
		if srcField.Type().AssignableTo(dstField.Type()) {
			dstField.Set(srcField)
		} else if srcField.Type().ConvertibleTo(dstField.Type()) {
			dstField.Set(srcField.Convert(dstField.Type()))
		}
	}
	return nil
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	r.index = len(r.data)
	return nil
}
