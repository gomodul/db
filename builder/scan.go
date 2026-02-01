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

// Scan scans the current row into dest
func (r *Rows) Scan(dest interface{}) error {
	if r.index < 0 || r.index >= len(r.data) {
		return ErrNotFound
	}
	// Would use the builder's scanning logic
	return nil
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	r.index = len(r.data)
	return nil
}
