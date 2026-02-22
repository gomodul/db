package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gomodul/db/callback"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/internal/security"
	"github.com/gomodul/db/logger"
	"github.com/gomodul/db/query"
	"github.com/gomodul/db/validation"
)

// QueryBuilder provides fluent API for building queries
type QueryBuilder struct {
	db  *DB
	q   *query.Query
	d   dialect.Dialect

	// Hooks
	hookExecutor *HookExecutor
	callbacks    *callback.CallbackRegistry

	// Validation
	validator validation.Validator
	validate  bool // Enable/disable auto-validation
}

// DB is a wrapper around the main db.DB for builder
type DB struct {
	Dialect      dialect.Dialect
	Caps         *dialect.Capabilities
	RowsAffected int64
	Error        error
}

// HookExecutor wraps the main hook executor
type HookExecutor struct {
	executor interface{}
}

// New creates a new QueryBuilder
func New(db *DB, model interface{}) *QueryBuilder {
	q := &query.Query{
		Model:     model,
		Context:   context.Background(),
		Operation: query.OpFind,
	}

	// Extract table name from model
	if model != nil {
		q.Collection = extractTableName(model)
	}

	return &QueryBuilder{
		db:        db,
		q:         q,
		d:         db.Dialect,
		callbacks: callback.NewCallbackRegistry(),
		validator: validation.NewValidator(),
		validate:  true, // Auto-validate by default
	}
}

// Model specifies the model to query
func (b *QueryBuilder) Model(model interface{}) *QueryBuilder {
	b.q.Model = model
	b.q.Collection = extractTableName(model)
	return b
}

// Table specifies the table name
func (b *QueryBuilder) Table(name string) *QueryBuilder {
	b.q.Collection = name
	return b
}

// Context sets the context for the query
func (b *QueryBuilder) Context(ctx context.Context) *QueryBuilder {
	b.q.Context = ctx
	return b
}

// WithTransaction sets the transaction ID
func (b *QueryBuilder) WithTransaction(txID string) *QueryBuilder {
	b.q.TxID = txID
	return b
}

// Clone creates a copy of the builder
func (b *QueryBuilder) Clone() *QueryBuilder {
	newQ := b.q.Clone()
	return &QueryBuilder{
		db: b.db,
		q:  newQ,
		d:  b.d,
	}
}

// ============ Query Building Methods ============

// Where adds a WHERE clause
func (b *QueryBuilder) Where(filter interface{}, args ...interface{}) *QueryBuilder {
	parsedFilter := b.parseFilter(filter, args...)
	b.q.Filters = append(b.q.Filters, parsedFilter)
	return b
}

// Or adds an OR condition
func (b *QueryBuilder) Or(filter interface{}, args ...interface{}) *QueryBuilder {
	parsedFilter := b.parseFilter(filter, args...)
	parsedFilter.Logic = query.LogicOr
	b.q.Filters = append(b.q.Filters, parsedFilter)
	return b
}

// And adds an AND condition (same as Where)
func (b *QueryBuilder) And(filter interface{}, args ...interface{}) *QueryBuilder {
	return b.Where(filter, args...)
}

// Not adds a NOT condition
func (b *QueryBuilder) Not(filter interface{}, args ...interface{}) *QueryBuilder {
	parsedFilter := b.parseFilter(filter, args...)
	parsedFilter.Logic = query.LogicNot
	b.q.Filters = append(b.q.Filters, parsedFilter)
	return b
}

// Order adds ORDER BY clause
func (b *QueryBuilder) Order(field string, direction ...query.SortDirection) *QueryBuilder {
	dir := query.DirAsc
	if len(direction) > 0 {
		dir = direction[0]
	}

	field, dir = parseOrderDirection(field)
	b.q.Orders = append(b.q.Orders, &query.Order{
		Field:     field,
		Direction: dir,
	})
	return b
}

// Limit adds LIMIT clause
func (b *QueryBuilder) Limit(limit int) *QueryBuilder {
	b.q.Limit = &limit
	return b
}

// Offset adds OFFSET clause
func (b *QueryBuilder) Offset(offset int) *QueryBuilder {
	b.q.Offset = &offset
	return b
}

// Select specifies fields to return
func (b *QueryBuilder) Select(fields ...string) *QueryBuilder {
	b.q.Selects = fields
	return b
}

// Join adds a JOIN clause
func (b *QueryBuilder) Join(collection string, conditions interface{}) *QueryBuilder {
	join := b.parseJoin(collection, conditions)
	b.q.Joins = append(b.q.Joins, join)
	return b
}

// LeftJoin adds a LEFT JOIN clause
func (b *QueryBuilder) LeftJoin(collection string, conditions interface{}) *QueryBuilder {
	join := b.parseJoin(collection, conditions)
	join.Type = query.JoinLeft
	b.q.Joins = append(b.q.Joins, join)
	return b
}

// RightJoin adds a RIGHT JOIN clause
func (b *QueryBuilder) RightJoin(collection string, conditions interface{}) *QueryBuilder {
	join := b.parseJoin(collection, conditions)
	join.Type = query.JoinRight
	b.q.Joins = append(b.q.Joins, join)
	return b
}

// InnerJoin adds an INNER JOIN clause
func (b *QueryBuilder) InnerJoin(collection string, conditions interface{}) *QueryBuilder {
	join := b.parseJoin(collection, conditions)
	join.Type = query.JoinInner
	b.q.Joins = append(b.q.Joins, join)
	return b
}

// Preload specifies relationships to eager load
func (b *QueryBuilder) Preload(fields ...string) *QueryBuilder {
	b.q.Preloads = append(b.q.Preloads, fields...)
	return b
}

// ============ Execution Methods ============

// Find executes a find query and scans results into dest
func (b *QueryBuilder) Find(dest interface{}) error {
	// Execute BeforeFind hooks
	ctx := callback.NewContext()
	if err := b.executeHook("before_query", dest, ctx); err != nil {
		return err
	}

	b.q.Operation = query.OpFind
	result, err := b.execute()
	if err != nil {
		return err
	}

	scanErr := b.scanResult(dest, result.Data)

	// Execute AfterFind hooks
	ctx = callback.NewContext()
	if err := b.executeHook("after_query", dest, ctx); err != nil {
		return err
	}

	return scanErr
}

// First finds the first matching record
func (b *QueryBuilder) First(dest interface{}) error {
	// Execute BeforeFind hooks
	ctx := callback.NewContext()
	if err := b.executeHook("before_query", dest, ctx); err != nil {
		return err
	}

	limit := 1
	b.q.Limit = &limit
	b.q.Operation = query.OpFind

	result, err := b.execute()
	if err != nil {
		return err
	}

	if len(result.Data) == 0 {
		return ErrNotFound
	}

	scanErr := b.scanResult(dest, result.Data[0:1])

	// Execute AfterFind hooks
	ctx = callback.NewContext()
	if err := b.executeHook("after_query", dest, ctx); err != nil {
		return err
	}

	return scanErr
}

// FirstOrCreate finds the first matching record or creates one
func (b *QueryBuilder) FirstOrCreate(dest interface{}, conds ...interface{}) error {
	err := b.First(dest)
	if err == ErrNotFound {
		return b.Create(dest)
	}
	return err
}

// FindOrCreate finds the first matching record or creates one
func (b *QueryBuilder) FindOrCreate(dest interface{}, conds ...interface{}) error {
	return b.FirstOrCreate(dest, conds...)
}

// Create inserts a new record
func (b *QueryBuilder) Create(value interface{}) error {
	// Validate if enabled
	if err := b.validateIfNeeded(value); err != nil {
		return err
	}

	// Execute BeforeCreate hooks
	ctx := callback.NewContext()
	if err := b.executeHook("before_create", value, ctx); err != nil {
		return err
	}
	if ctx.IsSkipped() {
		return nil
	}

	b.q.Operation = query.OpCreate
	b.q.Document = value

	result, err := b.execute()
	if err != nil {
		return err
	}

	// Store rows affected
	if result != nil {
		b.db.RowsAffected = result.RowsAffected
	}

	// Execute AfterCreate hooks
	ctx = callback.NewContext()
	if err := b.executeHook("after_create", value, ctx); err != nil {
		return err
	}

	return nil
}

// CreateInBatch inserts multiple records in batch
func (b *QueryBuilder) CreateInBatch(values interface{}, batchSize int) error {
	if !b.db.Caps.Query.BatchCreate {
		return b.createSequentially(values)
	}

	b.q.Operation = query.OpCreate
	b.q.Documents = extractSlice(values)

	_, err := b.execute()
	return err
}

// Save creates or updates a record (upsert)
func (b *QueryBuilder) Save(value interface{}) error {
	if b.hasPrimaryKey(value) {
		return b.Update(value)
	}
	return b.Create(value)
}

// Update modifies records
func (b *QueryBuilder) Update(values interface{}) error {
	// Validate if enabled
	if err := b.validateIfNeeded(values); err != nil {
		return err
	}

	// Execute BeforeUpdate hooks
	ctx := callback.NewContext()
	if err := b.executeHook("before_update", values, ctx); err != nil {
		return err
	}
	if ctx.IsSkipped() {
		return nil
	}

	b.q.Operation = query.OpUpdate
	b.q.Document = values

	result, err := b.execute()
	if err != nil {
		return err
	}

	// Store rows affected
	if result != nil {
		b.db.RowsAffected = result.RowsAffected
	}

	// Execute AfterUpdate hooks
	ctx = callback.NewContext()
	if err := b.executeHook("after_update", values, ctx); err != nil {
		return err
	}

	return nil
}

// Updates modifies records with map
func (b *QueryBuilder) Updates(values map[string]interface{}) error {
	// Map validation is skipped for Updates (flexible updates)
	// But you can call Validate manually before Updates if needed

	// Execute BeforeUpdate hooks
	ctx := callback.NewContext()
	if err := b.executeHook("before_update", values, ctx); err != nil {
		return err
	}
	if ctx.IsSkipped() {
		return nil
	}

	b.q.Operation = query.OpUpdate
	b.q.Updates = values

	result, err := b.execute()
	if err != nil {
		return err
	}

	// Store rows affected
	if result != nil {
		b.db.RowsAffected = result.RowsAffected
	}

	// Execute AfterUpdate hooks
	ctx = callback.NewContext()
	if err := b.executeHook("after_update", values, ctx); err != nil {
		return err
	}

	return nil
}

// UpdateColumn updates a single column
func (b *QueryBuilder) UpdateColumn(column string, value interface{}) error {
	return b.Updates(map[string]interface{}{column: value})
}

// Delete removes records
func (b *QueryBuilder) Delete() error {
	// Execute BeforeDelete hooks
	ctx := callback.NewContext()
	if err := b.executeHook("before_delete", b.q.Model, ctx); err != nil {
		return err
	}
	if ctx.IsSkipped() {
		return nil
	}

	b.q.Operation = query.OpDelete

	result, err := b.execute()
	if err != nil {
		return err
	}

	// Store rows affected
	if result != nil {
		b.db.RowsAffected = result.RowsAffected
	}

	// Execute AfterDelete hooks
	ctx = callback.NewContext()
	if err := b.executeHook("after_delete", b.q.Model, ctx); err != nil {
		return err
	}

	return nil
}

// ============ Internal Methods ============

// execute runs the query through the dialect
func (b *QueryBuilder) execute() (*dialect.Result, error) {
	if b.d == nil {
		return nil, fmt.Errorf("no dialect configured")
	}

	// Log query start
	ctx := b.q.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Get the SQL query if available (for SQL databases)
	sql := ""
	if b.q.IsRaw && b.q.Raw != "" {
		sql = b.q.Raw
	}

	// Log query start
	logger.Begin(ctx, sql, b.q.RawArgs...)

	// Execute query
	start := time.Now()
	result, err := b.d.Execute(ctx, b.q)
	duration := time.Since(start)

	// Log query end
	logger.End(ctx, sql, duration, err)

	return result, err
}

// executeHook executes a hook for the given operation
func (b *QueryBuilder) executeHook(operation string, model interface{}, ctx *callback.Context) error {
	if model == nil {
		return nil
	}

	// Convert model to pointer if it's a struct
	rv := reflect.ValueOf(model)
	if rv.Kind() != reflect.Ptr {
		// Create a pointer to the model
		ptr := reflect.New(rv.Type())
		ptr.Elem().Set(rv)
		model = ptr.Interface()
	}

	// Execute interface-based hooks
	switch operation {
	case "before_create":
		if bc, ok := model.(interface{ BeforeCreate(*callback.Context) error }); ok {
			if err := bc.BeforeCreate(ctx); err != nil {
				return err
			}
		}
	case "after_create":
		if ac, ok := model.(interface{ AfterCreate(*callback.Context) error }); ok {
			if err := ac.AfterCreate(ctx); err != nil {
				return err
			}
		}
	case "before_update":
		if bu, ok := model.(interface{ BeforeUpdate(*callback.Context) error }); ok {
			if err := bu.BeforeUpdate(ctx); err != nil {
				return err
			}
		}
	case "after_update":
		if au, ok := model.(interface{ AfterUpdate(*callback.Context) error }); ok {
			if err := au.AfterUpdate(ctx); err != nil {
				return err
			}
		}
	case "before_delete":
		if bd, ok := model.(interface{ BeforeDelete(*callback.Context) error }); ok {
			if err := bd.BeforeDelete(ctx); err != nil {
				return err
			}
		}
	case "after_delete":
		if ad, ok := model.(interface{ AfterDelete(*callback.Context) error }); ok {
			if err := ad.AfterDelete(ctx); err != nil {
				return err
			}
		}
	case "before_query":
		if bf, ok := model.(interface{ BeforeQuery(*callback.Context) error }); ok {
			if err := bf.BeforeQuery(ctx); err != nil {
				return err
			}
		}
	case "after_query":
		if af, ok := model.(interface{ AfterQuery(*callback.Context) error }); ok {
			if err := af.AfterQuery(ctx); err != nil {
				return err
			}
		}
	}

	// Execute registered callbacks through callback registry
	switch operation {
	case "before_create":
		if b.callbacks != nil {
			return b.callbacks.Create().BeforeCreate(model, ctx)
		}
	case "after_create":
		if b.callbacks != nil {
			return b.callbacks.Create().AfterCreate(model, ctx)
		}
	case "before_update":
		if b.callbacks != nil {
			return b.callbacks.Update().BeforeUpdate(model, ctx)
		}
	case "after_update":
		if b.callbacks != nil {
			return b.callbacks.Update().AfterUpdate(model, ctx)
		}
	case "before_delete":
		if b.callbacks != nil {
			return b.callbacks.Delete().BeforeDelete(model, ctx)
		}
	case "after_delete":
		if b.callbacks != nil {
			return b.callbacks.Delete().AfterDelete(model, ctx)
		}
	case "before_query":
		if b.callbacks != nil {
			return b.callbacks.Query().BeforeQuery(model, ctx)
		}
	case "after_query":
		if b.callbacks != nil {
			return b.callbacks.Query().AfterQuery(model, ctx)
		}
	}

	return nil
}

// scanResult scans the result data into the destination
func (b *QueryBuilder) scanResult(dest interface{}, data []interface{}) error {
	if len(data) == 0 {
		return ErrNotFound
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer")
	}

	destVal = destVal.Elem()

	// Handle slice destination
	if destVal.Kind() == reflect.Slice {
		return b.scanSlice(dest, data)
	}

	// Handle single value destination
	if len(data) > 0 {
		return b.scanValue(dest, data[0])
	}

	return nil
}

// scanSlice scans multiple results into a slice
func (b *QueryBuilder) scanSlice(dest interface{}, data []interface{}) error {
	destVal := reflect.ValueOf(dest)
	destVal = destVal.Elem()

	sliceType := destVal.Type()
	elemType := sliceType.Elem()

	slice := reflect.MakeSlice(sliceType, 0, len(data))

	for _, item := range data {
		elem := reflect.New(elemType).Elem()

		// Convert item to the destination type
		if err := b.convertValue(item, elem); err != nil {
			return err
		}

		slice = reflect.Append(slice, elem)
	}

	destVal.Set(slice)
	return nil
}

// scanValue scans a single value
func (b *QueryBuilder) scanValue(dest interface{}, value interface{}) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer")
	}

	destVal = destVal.Elem()
	return b.convertValue(value, destVal)
}

// convertValue converts source value to destination type
func (b *QueryBuilder) convertValue(src interface{}, dest reflect.Value) error {
	if src == nil {
		return nil
	}

	// If source is a map (from JSON/document DB), convert to struct
	if srcMap, ok := src.(map[string]interface{}); ok {
		return b.mapToStruct(srcMap, dest)
	}

	// Direct assignment if types are compatible
	srcVal := reflect.ValueOf(src)
	if srcVal.Type().ConvertibleTo(dest.Type()) {
		dest.Set(srcVal.Convert(dest.Type()))
		return nil
	}

	// Try JSON unmarshal
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest.Addr().Interface())
}

// mapToStruct converts a map to a struct
func (b *QueryBuilder) mapToStruct(m map[string]interface{}, dest reflect.Value) error {
	destType := dest.Type()
	if destType.Kind() != reflect.Struct {
		return fmt.Errorf("destination must be a struct")
	}

	// Handle pointer to struct
	if destType.Kind() == reflect.Ptr {
		destType = destType.Elem()
		if dest.IsNil() {
			dest.Set(reflect.New(destType))
			dest = dest.Elem()
		}
	}

	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		fieldName := getFieldName(field)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Check if map has this field
		if val, ok := m[fieldName]; ok {
			fieldVal := dest.FieldByName(field.Name)
			if fieldVal.IsValid() && fieldVal.CanSet() {
				if err := b.convertValue(val, fieldVal); err != nil {
					continue
				}
			}
		}
	}

	return nil
}

// ============ Helper Functions ============

func extractTableName(model interface{}) string {
	if model == nil {
		return ""
	}

	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Try to get TableName method
	if m, ok := reflect.New(t).Interface().(interface{ TableName() string }); ok {
		return m.TableName()
	}

	// Default: use struct name in lowercase
	return t.Name()
}

func extractSlice(values interface{}) []interface{} {
	v := reflect.ValueOf(values)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil
	}

	result := make([]interface{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		result[i] = v.Index(i).Interface()
	}
	return result
}

func hasNestedFilters(filters []*query.Filter) bool {
	for _, f := range filters {
		if len(f.Nested) > 0 {
			return true
		}
	}
	return false
}

func getFieldName(field reflect.StructField) string {
	// Check for db tag
	tag := field.Tag.Get("db")
	if tag != "" && tag != "-" {
		parts := strings.Split(tag, ",")
		return parts[0]
	}

	// Check for json tag
	tag = field.Tag.Get("json")
	if tag != "" && tag != "-" {
		parts := strings.Split(tag, ",")
		name := parts[0]
		if name != "" {
			return name
		}
	}

	// Default to field name
	return field.Name
}

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Array, reflect.Slice, reflect.Map, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	default:
		return reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
	}
}

func parseOrderDirection(field string) (string, query.SortDirection) {
	field = strings.TrimSpace(field)
	parts := strings.Fields(field)

	if len(parts) == 1 {
		return parts[0], query.DirAsc
	}

	direction := strings.ToUpper(parts[1])
	switch direction {
	case "DESC", "DESCENDING":
		return parts[0], query.DirDesc
	case "ASC", "ASCENDING":
		return parts[0], query.DirAsc
	default:
		return parts[0], query.DirAsc
	}
}

// parseFilter parses various filter formats into universal Filter
func (b *QueryBuilder) parseFilter(filter interface{}, args ...interface{}) *query.Filter {
	switch f := filter.(type) {
	case *query.Filter:
		return f
	case string:
		return b.parseStringFilter(f, args...)
	case map[string]interface{}:
		return b.parseMapFilter(f)
	case query.Filter:
		return &f
	default:
		// Try to parse as struct/model
		return b.parseStructFilter(filter)
	}
}

// parseStringFilter parses string filter with GORM-style dynamic detection
//
// Supported formats:
//   Where("id = ?", 1)                          // Simple equality
//   Where("age > ?", 18)                        // Comparison
//   Where("id IN ?", []int{1,2,3})              // Auto-detect IN from slice
//   Where("name LIKE ?", "John%")               // LIKE
//   Where("age BETWEEN ? AND ?", 18, 65)        // BETWEEN
//   Where("created_at > ? AND status = ?", t, "active")  // Multiple conditions
func (b *QueryBuilder) parseStringFilter(filterStr string, args ...interface{}) *query.Filter {
	parts := strings.Fields(filterStr)
	if len(parts) < 2 {
		// Simple "field ?" format - auto-detect operator from args
		if len(args) > 0 {
			return b.autoDetectFilter(filterStr, args...)
		}
		return &query.Filter{
			Field:    filterStr,
			Operator: query.OpEqual,
			Value:    args[0],
		}
	}

	field := parts[0]
	operatorStr := strings.ToUpper(parts[1])

	// Handle BETWEEN with AND
	if operatorStr == "BETWEEN" && len(args) >= 2 {
		return &query.Filter{
			Field:        field,
			Operator:     query.OpBetween,
			BetweenStart: args[0],
			BetweenEnd:   args[1],
			Logic:        query.LogicAnd,
		}
	}

	// Handle IN clause with slice or multiple args
	if operatorStr == "IN" {
		if len(args) == 1 && isSlice(args[0]) {
			return &query.Filter{
				Field:    field,
				Operator: query.OpIn,
				Values:   toSlice(args[0]),
				Logic:    query.LogicAnd,
			}
		}
		return &query.Filter{
			Field:    field,
			Operator: query.OpIn,
			Values:   args,
			Logic:    query.LogicAnd,
		}
	}

	// Handle NOT IN
	if operatorStr == "NOT" && len(parts) > 2 && strings.ToUpper(parts[2]) == "IN" {
		if len(args) == 1 && isSlice(args[0]) {
			return &query.Filter{
				Field:    field,
				Operator: query.OpNotIn,
				Values:   toSlice(args[0]),
				Logic:    query.LogicAnd,
			}
		}
		return &query.Filter{
			Field:    field,
			Operator: query.OpNotIn,
			Values:   args,
			Logic:    query.LogicAnd,
		}
	}

	var value interface{}
	if len(args) > 0 {
		value = args[0]
	}

	op := b.mapOperator(operatorStr)

	return &query.Filter{
		Field:    field,
		Operator: op,
		Value:    value,
		Logic:    query.LogicAnd,
	}
}

// autoDetectFilter auto-detects operator from argument type (GORM-style)
func (b *QueryBuilder) autoDetectFilter(field string, args ...interface{}) *query.Filter {
	if len(args) == 0 {
		return &query.Filter{
			Field:    field,
			Operator: query.OpEqual,
			Value:    nil,
		}
	}

	arg := args[0]

	// Detect IN from slice
	if isSlice(arg) {
		return &query.Filter{
			Field:    field,
			Operator: query.OpIn,
			Values:   toSlice(arg),
			Logic:    query.LogicAnd,
		}
	}

	// Detect nil/NULL
	if arg == nil {
		return &query.Filter{
			Field:    field,
			Operator: query.OpNull,
			Logic:    query.LogicAnd,
		}
	}

	return &query.Filter{
		Field:    field,
		Operator: query.OpEqual,
		Value:    arg,
		Logic:    query.LogicAnd,
	}
}

// isSlice checks if value is a slice or array
func isSlice(v interface{}) bool {
	if v == nil {
		return false
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array
}

// toSlice converts interface{} to []interface{}
func toSlice(v interface{}) []interface{} {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		result := make([]interface{}, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			result[i] = rv.Index(i).Interface()
		}
		return result
	}
	return []interface{}{v}
}

// parseMapFilter parses map[string]interface{} filter
func (b *QueryBuilder) parseMapFilter(m map[string]interface{}) *query.Filter {
	if len(m) == 0 {
		return nil
	}

	// For single key-value pair, return simple filter
	if len(m) == 1 {
		for field, value := range m {
			return &query.Filter{
				Field:    field,
				Operator: query.OpEqual,
				Value:    value,
				Logic:    query.LogicAnd,
			}
		}
	}

	// For multiple key-value pairs, combine with AND
	filters := make([]*query.Filter, 0, len(m))
	for field, value := range m {
		filters = append(filters, &query.Filter{
			Field:    field,
			Operator: query.OpEqual,
			Value:    value,
			Logic:    query.LogicAnd,
		})
	}

	return &query.Filter{
		Logic:  query.LogicAnd,
		Nested: filters,
	}
}

// parseStructFilter parses struct as filter
func (b *QueryBuilder) parseStructFilter(structObj interface{}) *query.Filter {
	v := reflect.ValueOf(structObj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil
	}

	t := v.Type()
	filters := make([]*query.Filter, 0)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		structField := t.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		fieldName := getFieldName(structField)
		if fieldName == "-" {
			continue
		}

		// Skip zero values
		if isZero(field) {
			continue
		}

		filters = append(filters, &query.Filter{
			Field:    fieldName,
			Operator: query.OpEqual,
			Value:    field.Interface(),
			Logic:    query.LogicAnd,
		})
	}

	if len(filters) == 0 {
		return nil
	}

	if len(filters) == 1 {
		return filters[0]
	}

	return &query.Filter{
		Logic:  query.LogicAnd,
		Nested: filters,
	}
}

// mapOperator maps operator string to FilterOperator
func (b *QueryBuilder) mapOperator(op string) query.FilterOperator {
	switch op {
	case "=":
		return query.OpEqual
	case "!=", "<>":
		return query.OpNotEqual
	case ">":
		return query.OpGreaterThan
	case ">=":
		return query.OpGreaterOrEqual
	case "<":
		return query.OpLessThan
	case "<=":
		return query.OpLessOrEqual
	case "IN":
		return query.OpIn
	case "NOT IN":
		return query.OpNotIn
	case "LIKE":
		return query.OpLike
	case "NOT LIKE":
		return query.OpNotLike
	case "BETWEEN":
		return query.OpBetween
	case "IS NULL":
		return query.OpNull
	case "IS NOT NULL":
		return query.OpNotNull
	default:
		return query.OpEqual
	}
}

// parseJoin parses join parameters
func (b *QueryBuilder) parseJoin(collection string, conditions interface{}) *query.Join {
	join := &query.Join{
		Collection: collection,
		Type:       query.JoinInner,
	}

	switch cond := conditions.(type) {
	case *query.Join:
		return cond
	case query.Join:
		return &cond
	case string:
		join.Conditions = []*query.Filter{
			b.parseStringFilter(cond),
		}
	case map[string]interface{}:
		if len(cond) == 1 {
			for foreignKey, reference := range cond {
				join.ForeignKeys = []string{foreignKey}
				if refStr, ok := reference.(string); ok {
					join.References = []string{refStr}
				}
			}
		}
	}

	return join
}

// createSequentially creates records one by one (fallback)
func (b *QueryBuilder) createSequentially(values interface{}) error {
	vals := reflect.ValueOf(values)
	if vals.Kind() != reflect.Slice && vals.Kind() != reflect.Array {
		return fmt.Errorf("values must be a slice or array")
	}

	for i := 0; i < vals.Len(); i++ {
		item := vals.Index(i).Interface()

		// Reset operation for each item
		b.q.Operation = query.OpCreate
		b.q.Document = item

		_, err := b.execute()
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *QueryBuilder) hasPrimaryKey(model interface{}) bool {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return false
	}

	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("db")

		if strings.Contains(tag, ",pk") || strings.Contains(tag, "primarykey") || tag == "pk" {
			fieldVal := val.Field(i)
			return !isZero(fieldVal)
		}

		if field.Name == "ID" || field.Name == "Id" {
			fieldVal := val.Field(i)
			return !isZero(fieldVal)
		}
	}

	return false
}

// ============ Filter Helper Functions ============
//
// RECOMMENDED: Use Where() for all filtering - it supports GORM-style dynamic detection:
//
// Where("id = ?", 1)                           // Simple equality
// Where("age > ?", 18)                         // Comparison
// Where("id IN ?", []int{1,2,3})               // Auto-detect IN from slice
// Where("name LIKE ?", "John%")                // LIKE
// Where("age BETWEEN ? AND ?", 18, 65)         // BETWEEN
// Where(map[string]interface{}{"status": "active"}) // Map filter
// Where(User{Name: "John", Age: 30})           // Struct filter
//
// The methods below are provided as shortcuts for common cases.

// In adds an IN filter - shorthand for Where("field IN ?", values)
//
// Prefer: Where("id IN ?", []int{1,2,3})
func (b *QueryBuilder) In(field string, values ...interface{}) *QueryBuilder {
	return b.Where(field+" IN ?", values...)
}

// NotIn adds a NOT IN filter - shorthand for Where("field NOT IN ?", values)
//
// Prefer: Where("id NOT IN ?", []int{1,2,3})
func (b *QueryBuilder) NotIn(field string, values ...interface{}) *QueryBuilder {
	return b.Where(field+" NOT IN ?", values...)
}

// WhereIn adds an IN filter (legacy: use In or Where instead)
func (b *QueryBuilder) WhereIn(field string, values ...interface{}) *QueryBuilder {
	return b.In(field, values...)
}

// WhereNotIn adds a NOT IN filter (legacy: use NotIn or Where instead)
func (b *QueryBuilder) WhereNotIn(field string, values ...interface{}) *QueryBuilder {
	return b.NotIn(field, values...)
}

// Between adds a BETWEEN filter - shorthand for Where("field BETWEEN ? AND ?", start, end)
//
// Prefer: Where("age BETWEEN ? AND ?", 18, 65)
func (b *QueryBuilder) Between(field string, start, end interface{}) *QueryBuilder {
	return b.Where(field+" BETWEEN ? AND ?", start, end)
}

// WhereBetween adds a BETWEEN filter (legacy: use Between or Where instead)
func (b *QueryBuilder) WhereBetween(field string, start, end interface{}) *QueryBuilder {
	return b.Between(field, start, end)
}

// Null adds an IS NULL filter - shorthand for Where("field IS NULL")
//
// Prefer: Where("deleted_at IS NULL")
func (b *QueryBuilder) Null(field string) *QueryBuilder {
	return b.Where(field + " IS NULL")
}

// WhereNull adds an IS NULL filter (legacy: use Null or Where instead)
func (b *QueryBuilder) WhereNull(field string) *QueryBuilder {
	return b.Null(field)
}

// NotNull adds an IS NOT NULL filter - shorthand for Where("field IS NOT NULL")
//
// Prefer: Where("deleted_at IS NOT NULL")
func (b *QueryBuilder) NotNull(field string) *QueryBuilder {
	return b.Where(field + " IS NOT NULL")
}

// WhereNotNull adds an IS NOT NULL filter (legacy: use NotNull or Where instead)
func (b *QueryBuilder) WhereNotNull(field string) *QueryBuilder {
	return b.NotNull(field)
}

// Like adds a LIKE filter - shorthand for Where("field LIKE ?", pattern)
//
// Prefer: Where("name LIKE ?", "John%")
func (b *QueryBuilder) Like(field, pattern string) *QueryBuilder {
	return b.Where(field+" LIKE ?", pattern)
}

// WhereLike adds a LIKE filter (legacy: use Like or Where instead)
func (b *QueryBuilder) WhereLike(field, pattern string) *QueryBuilder {
	return b.Like(field, pattern)
}

// NotLike adds a NOT LIKE filter - shorthand for Where("field NOT LIKE ?", pattern)
func (b *QueryBuilder) NotLike(field, pattern string) *QueryBuilder {
	return b.Where(field+" NOT LIKE ?", pattern)
}

// OrIn adds an OR IN condition
func (b *QueryBuilder) OrIn(field string, values ...interface{}) *QueryBuilder {
	return b.Or(&query.Filter{
		Field:    field,
		Operator: query.OpIn,
		Values:   values,
	})
}

// ============ Pagination Methods ============

// Paginate sets both limit and offset for pagination
func (b *QueryBuilder) Paginate(page, perPage int) *QueryBuilder {
	offset := (page - 1) * perPage
	b.q.Offset = &offset
	b.q.Limit = &perPage
	return b
}

// Page sets the page number (1-based)
func (b *QueryBuilder) Page(page int) *QueryBuilder {
	perPage := 10 // Default
	offset := (page - 1) * perPage
	b.q.Offset = &offset
	b.q.Limit = &perPage
	return b
}

// PerPage sets the number of records per page
func (b *QueryBuilder) PerPage(perPage int) *QueryBuilder {
	b.q.Limit = &perPage
	return b
}

// ============ Raw Query Methods ============

// Raw executes a raw query
//
// SECURITY WARNING: Always use parameterized queries with placeholders.
//   ✅ GOOD: db.Raw("SELECT * FROM users WHERE id = ?", userID)
//   ❌ BAD:  db.Raw("SELECT * FROM users WHERE id = " + userID)
func (b *QueryBuilder) Raw(sql string, args ...interface{}) *QueryBuilder {
	// Validate raw query for security
	if warnings, err := security.ValidateRawQuery(sql, nil); err != nil {
		b.db.Error = fmt.Errorf("raw query validation failed: %w", err)
		return b
	} else if len(warnings) > 0 && b.db != nil {
		// Store warnings for later logging
		b.q.Warnings = make([]interface{}, len(warnings))
		for i, w := range warnings {
			b.q.Warnings[i] = w
		}
	}

	b.q.IsRaw = true
	b.q.Raw = sql
	b.q.RawArgs = args
	return b
}

// Exec executes a query without returning rows
//
// SECURITY WARNING: Always use parameterized queries with placeholders.
//   ✅ GOOD: db.Exec("UPDATE users SET name = ? WHERE id = ?", "John", 1)
//   ❌ BAD:  db.Exec("UPDATE users SET name = '" + name + "' WHERE id = " + id)
func (b *QueryBuilder) Exec(sql string, args ...interface{}) (int64, error) {
	// Validate raw query for security
	if _, err := security.ValidateRawQuery(sql, nil); err != nil {
		return 0, fmt.Errorf("raw query validation failed: %w", err)
	}

	b.q.IsRaw = true
	b.q.Raw = sql
	b.q.RawArgs = args
	b.q.Operation = query.OpUpdate

	result, err := b.execute()
	if err != nil {
		return 0, err
	}
	return result.RowsAffected, nil
}

// ============ Transaction Methods ============

// Transaction executes a function within a transaction
func (b *QueryBuilder) Transaction(fn func(tx *QueryBuilder) error) error {
	if !b.db.Caps.Transaction.Supported {
		return ErrNotSupported
	}

	tx, err := dialect.BeginTx(b.q.Context, b.d)
	if err != nil {
		return err
	}

	txB := &QueryBuilder{
		db: b.db,
		q:  b.q.Clone().WithTransaction("tx"),
		d:  b.d,
	}

	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()

	if err := fn(txB); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// BeginTx starts a new transaction
func (b *QueryBuilder) BeginTx() (*Tx, error) {
	if !b.db.Caps.Transaction.Supported {
		return nil, ErrNotSupported
	}

	tx, err := dialect.BeginTx(b.q.Context, b.d)
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx: tx,
		b:  b,
	}, nil
}

// Tx represents a database transaction
type Tx struct {
	tx dialect.Transaction
	b  *QueryBuilder
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

// Tx returns a QueryBuilder for this transaction
func (tx *Tx) Tx() *QueryBuilder {
	return tx.b.Clone().WithTransaction("tx")
}

// ============ Validation Methods ============

// Validate validates the model
func (b *QueryBuilder) Validate(model interface{}) error {
	if b.validator == nil {
		return nil
	}
	return b.validator.Validate(model)
}

// ValidateField validates a single field
func (b *QueryBuilder) ValidateField(field interface{}, tag string) error {
	if b.validator == nil {
		return nil
	}
	return b.validator.ValidateField(field, tag)
}

// SkipValidation disables auto-validation for the next operation
func (b *QueryBuilder) SkipValidation() *QueryBuilder {
	b.validate = false
	return b
}

// EnableValidation enables auto-validation
func (b *QueryBuilder) EnableValidation() *QueryBuilder {
	b.validate = true
	return b
}

// SetValidator sets a custom validator
func (b *QueryBuilder) SetValidator(validator validation.Validator) *QueryBuilder {
	b.validator = validator
	return b
}

// validateIfNeeded validates the model if validation is enabled
func (b *QueryBuilder) validateIfNeeded(model interface{}) error {
	if b.validate && b.validator != nil {
		return b.validator.Validate(model)
	}
	return nil
}

// ============ Exported Methods for External Access ============

// GetQuery returns a copy of the internal query state
// This allows external packages to inspect the query configuration
func (b *QueryBuilder) GetQuery() *query.Query {
	if b.q == nil {
		return nil
	}
	return b.q.Clone()
}

// GetDialect returns the dialect being used
func (b *QueryBuilder) GetDialect() dialect.Dialect {
	return b.d
}

// GetCapabilities returns the driver capabilities
func (b *QueryBuilder) GetCapabilities() *dialect.Capabilities {
	return b.db.Caps
}

// GetOperation returns the current operation type
func (b *QueryBuilder) GetOperation() string {
	if b.q == nil {
		return ""
	}
	return string(b.q.Operation)
}

// GetCollection returns the table/collection name
func (b *QueryBuilder) GetCollection() string {
	if b.q == nil {
		return ""
	}
	return b.q.Collection
}

// GetLimit returns the limit value
func (b *QueryBuilder) GetLimit() int {
	if b.q == nil || b.q.Limit == nil {
		return 0
	}
	return *b.q.Limit
}

// GetOffset returns the offset value
func (b *QueryBuilder) GetOffset() int {
	if b.q == nil || b.q.Offset == nil {
		return 0
	}
	return *b.q.Offset
}

// ExecuteQuery executes the current query and returns the result
// This is exported for use by streaming and other external packages
func (b *QueryBuilder) ExecuteQuery(ctx context.Context) (*dialect.Result, error) {
	if ctx != nil && b.q != nil {
		b.q.Context = ctx
	}
	return b.execute()
}

// ExecuteResult executes the query and returns the dialect.Result
// This is a convenience method for external packages
func (b *QueryBuilder) ExecuteResult() (*dialect.Result, error) {
	return b.execute()
}

// ============ Advanced Query Features (CTE, Subquery, Window Functions) ============

// WithCTE adds a Common Table Expression (WITH clause)
//
// Example:
//
//	db.Model(&User{}).WithCTE("active_users", `
//		SELECT id, name FROM users WHERE status = 'active'
//	`)
//
//	// Or with a subquery builder:
//	subQuery := db.Model(&User{}).Where("status = ?", "active")
//	db.Model(&Order{}).WithCTE("active_users", subQuery)
func (b *QueryBuilder) WithCTE(name string, queryParam interface{}, columns ...string) *QueryBuilder {
	cte := &query.CTE{
		Name:    name,
		Columns: columns,
	}

	// Handle different query types
	switch q := queryParam.(type) {
	case *query.Query:
		cte.Query = q
	case *QueryBuilder:
		cte.Query = q.q
	case string:
		// Raw SQL for CTE
		cte.Query = &query.Query{
			Raw:    q,
			IsRaw: true,
		}
	default:
		b.db.Error = fmt.Errorf("invalid CTE query type: %T", queryParam)
		return b
	}

	if b.q.CTEs == nil {
		b.q.CTEs = make([]*query.CTE, 0)
	}
	b.q.CTEs = append(b.q.CTEs, cte)
	return b
}

// WithRecursiveCTE adds a recursive Common Table Expression
//
// Example:
//
//	db.Model(&Category{}).WithRecursiveCTE("category_tree", `
//		SELECT id, name, parent_id FROM categories WHERE parent_id IS NULL
//		UNION ALL
//		SELECT c.id, c.name, c.parent_id FROM categories c
//		INNER JOIN category_tree ct ON c.parent_id = ct.id
//	`)
func (b *QueryBuilder) WithRecursiveCTE(name string, rawSQL string, columns ...string) *QueryBuilder {
	cte := &query.CTE{
		Name:      name,
		Recursive: true,
		Columns:   columns,
		Query: &query.Query{
			Raw:    rawSQL,
			IsRaw: true,
		},
	}

	if b.q.CTEs == nil {
		b.q.CTEs = make([]*query.CTE, 0)
	}
	b.q.CTEs = append(b.q.CTEs, cte)
	return b
}

// WhereSubquery adds a subquery in the WHERE clause
//
// Example:
//
//	// WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
//	subQuery := db.Model(&Order{}).Select("user_id").Where("total > ?", 100)
//	db.Model(&User{}).WhereSubquery("id", "IN", subQuery)
//
//	// WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
//	subQuery := db.Model(&Order{}).Where("orders.user_id = users.id")
//	db.Model(&User{}).WhereSubquery("", "EXISTS", subQuery)
func (b *QueryBuilder) WhereSubquery(field, operator string, subQuery *QueryBuilder) *QueryBuilder {
	if b.q.Subqueries == nil {
		b.q.Subqueries = make([]*query.Subquery, 0)
	}

	sq := &query.Subquery{
		Query:    subQuery.q,
		Type:     query.SubqueryWhere,
		Operator: operator,
		Field:    field,
	}

	b.q.Subqueries = append(b.q.Subqueries, sq)
	return b
}

// FromSubquery uses a subquery as the FROM source
//
// Example:
//
//	// SELECT * FROM (SELECT * FROM users WHERE age > 18) AS adults
//	subQuery := db.Model(&User{}).Where("age > ?", 18)
//	db.Model("").FromSubquery("adults", subQuery)
func (b *QueryBuilder) FromSubquery(alias string, subQuery *QueryBuilder) *QueryBuilder {
	if b.q.Subqueries == nil {
		b.q.Subqueries = make([]*query.Subquery, 0)
	}

	sq := &query.Subquery{
		Query: subQuery.q,
		Type:  query.SubqueryFrom,
		Alias: alias,
	}

	b.q.Subqueries = append(b.q.Subqueries, sq)
	b.q.Collection = alias // Update collection to use subquery alias
	return b
}

// Window adds a window function to the query
//
// Example:
//
//	// ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)
//	db.Model(&Employee{}).
//		Window("row_num", "ROW_NUMBER", "").
//		PartitionBy("department").
//		WindowOrderBy("salary", false).
//		WindowFrame("ROWS", "UNBOUNDED PRECEDING", "CURRENT ROW", "")
func (b *QueryBuilder) Window(alias, funcName, expression string) *QueryBuilder {
	wf := &query.WindowFunc{
		Func:       funcName,
		Alias:      alias,
		Expression: expression,
	}

	if b.q.WindowFuncs == nil {
		b.q.WindowFuncs = make([]*query.WindowFunc, 0)
	}
	b.q.WindowFuncs = append(b.q.WindowFuncs, wf)
	return b
}

// PartitionBy adds a PARTITION BY clause to the last window function
//
// Example:
//
//	db.Model(&Employee{}).
//		Window("row_num", "ROW_NUMBER", "").
//		PartitionBy("department", "team")
func (b *QueryBuilder) PartitionBy(columns ...string) *QueryBuilder {
	if len(b.q.WindowFuncs) == 0 {
		b.db.Error = fmt.Errorf("no window function defined for PartitionBy")
		return b
	}

	lastWF := b.q.WindowFuncs[len(b.q.WindowFuncs)-1]
	lastWF.Partition = append(lastWF.Partition, columns...)
	return b
}

// WindowOrderBy adds an ORDER BY clause to the last window function
//
// Example:
//
//	db.Model(&Employee{}).
//		Window("row_num", "ROW_NUMBER", "").
//		WindowOrderBy("salary", false) // ORDER BY salary DESC
func (b *QueryBuilder) WindowOrderBy(field string, ascending bool) *QueryBuilder {
	if len(b.q.WindowFuncs) == 0 {
		b.db.Error = fmt.Errorf("no window function defined for WindowOrderBy")
		return b
	}

	lastWF := b.q.WindowFuncs[len(b.q.WindowFuncs)-1]
	if lastWF.OrderBy == nil {
		lastWF.OrderBy = make([]*query.Order, 0)
	}

	order := &query.Order{
		Field:     field,
		Direction: query.DirAsc,
	}
	if !ascending {
		order.Direction = query.DirDesc
	}

	lastWF.OrderBy = append(lastWF.OrderBy, order)
	return b
}

// WindowFrame adds a frame clause to the last window function
//
// Example:
//
//	db.Model(&Employee{}).
//		Window("row_num", "ROW_NUMBER", "").
//		WindowFrame("ROWS", "UNBOUNDED PRECEDING", "CURRENT ROW", "")
//
//	// ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
//	db.Model(&Employee{}).
//		Window("avg_salary", "AVG", "salary").
//		WindowFrame("ROWS", "1 PRECEDING", "1 FOLLOWING", "")
func (b *QueryBuilder) WindowFrame(mode, start, end, exclude string) *QueryBuilder {
	if len(b.q.WindowFuncs) == 0 {
		b.db.Error = fmt.Errorf("no window function defined for WindowFrame")
		return b
	}

	lastWF := b.q.WindowFuncs[len(b.q.WindowFuncs)-1]
	lastWF.Frame = &query.Frame{
		Mode:    mode,
		Start:   start,
		End:     end,
		Exclude: exclude,
	}
	return b
}

// Rank adds a RANK() window function
//
// Example:
//
//	db.Model(&Employee{}).
//		Rank("salary_rank", "").
//		PartitionBy("department").
//		WindowOrderBy("salary", false)
func (b *QueryBuilder) Rank(alias string) *QueryBuilder {
	return b.Window(alias, "RANK", "")
}

// DenseRank adds a DENSE_RANK() window function
//
// Example:
//
//	db.Model(&Employee{}).
//		DenseRank("salary_rank").
//		WindowOrderBy("salary", false)
func (b *QueryBuilder) DenseRank(alias string) *QueryBuilder {
	return b.Window(alias, "DENSE_RANK", "")
}

// RowNumber adds a ROW_NUMBER() window function
//
// Example:
//
//	db.Model(&Employee{}).
//		RowNumber("row_num").
//		PartitionBy("department").
//		WindowOrderBy("hire_date", false)
func (b *QueryBuilder) RowNumber(alias string) *QueryBuilder {
	return b.Window(alias, "ROW_NUMBER", "")
}

// Lag adds a LAG() window function for accessing previous row values
//
// Example:
//
//	db.Model(&Sales{}).
//		Lag("prev_sales", "amount", 1).
//		PartitionBy("product_id").
//		WindowOrderBy("sale_date", false)
func (b *QueryBuilder) Lag(alias, column string, offset int) *QueryBuilder {
	expr := column
	if offset > 0 {
		expr = fmt.Sprintf("%s, %d", column, offset)
	}
	return b.Window(alias, "LAG", expr)
}

// Lead adds a LEAD() window function for accessing next row values
//
// Example:
//
//	db.Model(&Sales{}).
//		Lead("next_sales", "amount", 1).
//		PartitionBy("product_id").
//		WindowOrderBy("sale_date", false)
func (b *QueryBuilder) Lead(alias, column string, offset int) *QueryBuilder {
	expr := column
	if offset > 0 {
		expr = fmt.Sprintf("%s, %d", column, offset)
	}
	return b.Window(alias, "LEAD", expr)
}
