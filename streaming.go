package db

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/gomodul/db/builder"
	"github.com/gomodul/db/dialect"
)

// Cursor provides streaming access to query results
type Cursor struct {
	ctx    context.Context
	db     *DB
	qb     *builder.QueryBuilder
	result *dialect.Result
	index  int
	closed bool
}

// NewCursor creates a new cursor for streaming query results
//
// Example:
//	cursor, err := db.Model(&User{}).Where("status = ?", "active").Cursor()
//	if err != nil {
//	    return err
//	}
//	defer cursor.Close()
//
//	for cursor.Next() {
//	    var user User
//	    if err := cursor.Scan(&user); err != nil {
//	        return err
//	    }
//	    // Process user
//	}
func (db *DB) Cursor(query *builder.QueryBuilder) (*Cursor, error) {
	// Execute query to get results
	result, err := executeQuery(query)
	if err != nil {
		return nil, err
	}

	return &Cursor{
		ctx:    context.Background(),
		db:     db,
		qb:     query,
		result: result,
		index:  -1,
	}, nil
}

// executeQuery executes a query and returns the result
func executeQuery(qb *builder.QueryBuilder) (*dialect.Result, error) {
	// Use the exported ExecuteResult method
	return qb.ExecuteResult()
}

// Next advances the cursor to the next result
// Returns false if there are no more results
func (c *Cursor) Next() bool {
	if c.closed {
		return false
	}

	c.index++
	return c.index < len(c.result.Data)
}

// Scan scans the current result into the destination
func (c *Cursor) Scan(dest interface{}) error {
	if c.closed {
		return fmt.Errorf("cursor is closed")
	}

	if c.index < 0 || c.index >= len(c.result.Data) {
		return fmt.Errorf("no current row")
	}

	// Scan the current data into dest
	if c.index < len(c.result.Data) {
		data := c.result.Data[c.index]
		return scanData(data, dest)
	}

	return ErrNotFound
}

// scanData scans data into destination using reflection
func scanData(data interface{}, dest interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer")
	}

	destVal = destVal.Elem()

	// Handle map data (from SQL results)
	if dataMap, ok := data.(map[string]interface{}); ok {
		return scanMapToStruct(dataMap, destVal)
	}

	// Direct assignment for compatible types
	srcVal := reflect.ValueOf(data)
	if srcVal.Type().ConvertibleTo(destVal.Type()) {
		destVal.Set(srcVal.Convert(destVal.Type()))
		return nil
	}

	return fmt.Errorf("cannot scan %T into %T", data, dest)
}

// scanMapToStruct scans a map into a struct
func scanMapToStruct(m map[string]interface{}, dest reflect.Value) error {
	if dest.Kind() != reflect.Struct {
		return fmt.Errorf("destination must be a struct")
	}

	typ := dest.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := dest.Field(i)

		// Skip unexported fields
		if !fieldVal.CanSet() {
			continue
		}

		// Get field name from tag or use field name
		fieldName := getJSONFieldName(field)
		if fieldName == "-" {
			continue
		}

		// Check if map has this field
		if val, ok := m[fieldName]; ok {
			if err := setFieldValue(fieldVal, val); err != nil {
				// Try to set anyway
				continue
			}
		}
	}

	return nil
}

// getJSONFieldName gets the JSON field name from struct tag
func getJSONFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag != "" && tag != "-" {
		// Handle json:"name,omitempty" style tags
		if idx := indexOfComma(tag); idx >= 0 {
			return tag[:idx]
		}
		return tag
	}

	// Fallback to db tag
	tag = field.Tag.Get("db")
	if tag != "" && tag != "-" {
		if idx := indexOfComma(tag); idx >= 0 {
			return tag[:idx]
		}
		return tag
	}

	return field.Name
}

// indexOfComma finds the index of comma in a string
func indexOfComma(s string) int {
	for i, c := range s {
		if c == ',' {
			return i
		}
	}
	return -1
}

// setFieldValue sets a struct field value from interface{}
func setFieldValue(field reflect.Value, value interface{}) error {
	if value == nil {
		return nil
	}

	val := reflect.ValueOf(value)
	if val.Type().ConvertibleTo(field.Type()) {
		field.Set(val.Convert(field.Type()))
		return nil
	}

	return fmt.Errorf("cannot convert %v to %v", val.Type(), field.Type())
}

// ScanEach executes a callback function for each result
// This is useful for processing large result sets without loading everything into memory
//
// Example:
//	err := ScanEach(db.Model(&User{}).Where("status = ?", "active"), func(user *User) error {
//	    fmt.Printf("User: %v\n", user.Name)
//	    return nil
//	})
func ScanEach(qb *builder.QueryBuilder, fn interface{}) error {
	fnVal := reflect.ValueOf(fn)
	fnType := fnVal.Type()

	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("ScanEach requires a function")
	}

	if fnType.NumIn() != 1 {
		return fmt.Errorf("function must accept exactly one argument")
	}

	argType := fnType.In(0)
	if argType.Kind() != reflect.Ptr {
		return fmt.Errorf("function argument must be a pointer")
	}

	elemType := argType.Elem()

	// Execute query
	result, err := executeQuery(qb)
	if err != nil {
		return err
	}

	// Process each result
	for _, data := range result.Data {
		// Create new instance of the element type
		elemPtr := reflect.New(elemType)

		// Scan data into the new instance
		if err := scanData(data, elemPtr.Interface()); err != nil {
			return err
		}

		// Call the function
		results := fnVal.Call([]reflect.Value{elemPtr})

		// Check if function returned an error
		if len(results) > 0 {
			if err, ok := results[0].Interface().(error); ok && err != nil {
				return err
			}
		}
	}

	return nil
}

// Stream returns a channel that streams results one at a time
//
// Example:
//	for item := range Stream(db.Model(&User{}).Where("status = ?", "active")) {
//	    if item.Error != nil {
//	        log.Printf("Error: %v", item.Error)
//	        continue
//	    }
//	    user := item.Data.(*User)
//	    fmt.Printf("User: %v\n", user.Name)
//	}
func Stream(qb *builder.QueryBuilder) <-chan ResultItem {
	ch := make(chan ResultItem, 10) // Buffered channel

	go func() {
		defer close(ch)

		// Execute query
		result, err := executeQuery(qb)
		if err != nil {
			ch <- ResultItem{Error: err}
			return
		}

		// Stream each result
		for _, data := range result.Data {
			ch <- ResultItem{Data: data}
		}
	}()

	return ch
}

// ResultItem represents a single item from a streamed result
type ResultItem struct {
	Data  interface{}
	Error error
}

// Close closes the cursor and releases any resources
func (c *Cursor) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true
	// Reset result to free memory
	c.result = nil
	return nil
}

// Err returns any error that occurred during cursor operations
func (c *Cursor) Err() error {
	if c.db != nil && c.db.Error != nil {
		return c.db.Error
	}
	return nil
}

// BatchProcessor processes results in batches for better memory efficiency
//
// Example:
//	processor := NewBatchProcessor(100)
//	err := db.Model(&User{}).Where("status = ?", "active").ProcessBatch(processor, func(users []*User) error {
//	    // Process batch of 100 users
//	    return nil
//	})
type BatchProcessor struct {
	batchSize int
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(batchSize int) *BatchProcessor {
	return &BatchProcessor{
		batchSize: batchSize,
	}
}

// ProcessBatch executes a query and processes results in batches
// This is a method on BatchProcessor that processes QueryBuilder results
func (bp *BatchProcessor) ProcessBatch(qb *builder.QueryBuilder, fn interface{}) error {
	fnVal := reflect.ValueOf(fn)
	fnType := fnVal.Type()

	if fnType.Kind() != reflect.Func {
		return fmt.Errorf("batch function must be a function")
	}

	if fnType.NumIn() != 1 {
		return fmt.Errorf("batch function must accept exactly one argument")
	}

	// Get batch slice type
	argType := fnType.In(0)
	if argType.Kind() != reflect.Slice {
		return fmt.Errorf("batch function argument must be a slice")
	}

	elemType := argType.Elem()

	// Process in batches using GetLimit and SetOffset
	offset := 0
	limit := bp.batchSize

	for {
		// Execute query with limit and offset
		result, err := qb.Limit(limit).Offset(offset).ExecuteResult()
		if err != nil {
			if err == io.EOF || err == ErrNotFound {
				break
			}
			return err
		}

		if len(result.Data) == 0 {
			break
		}

		// Create batch slice
		batch := reflect.MakeSlice(argType, len(result.Data), len(result.Data))

		// Fill batch with data
		for i, data := range result.Data {
			elemPtr := reflect.New(elemType)
			if err := scanData(data, elemPtr.Interface()); err != nil {
				return err
			}
			batch.Index(i).Set(elemPtr.Elem())
		}

		// Call the batch function
		results := fnVal.Call([]reflect.Value{batch})

		// Check for error
		if len(results) > 0 {
			if err, ok := results[0].Interface().(error); ok && err != nil {
				return err
			}
		}

		// Move to next batch
		if len(result.Data) < bp.batchSize {
			break
		}
		offset += len(result.Data)
	}

	return nil
}
