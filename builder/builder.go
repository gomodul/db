package builder

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// QueryBuilder provides fluent API for building queries
type QueryBuilder struct {
	db  *DB
	q   *query.Query
	d   dialect.Dialect
}

// DB is a wrapper around the main db.DB for builder
type DB struct {
	Dialect      dialect.Dialect
	Caps         *dialect.Capabilities
	RowsAffected int64
	Error        error
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
		db: db,
		q:  q,
		d:  db.Dialect,
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
	b.q.Operation = query.OpFind
	result, err := b.execute()
	if err != nil {
		return err
	}
	return b.scanResult(dest, result.Data)
}

// First finds the first matching record
func (b *QueryBuilder) First(dest interface{}) error {
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

	return b.scanResult(dest, result.Data[0:1])
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
	b.q.Operation = query.OpCreate
	b.q.Document = value

	result, err := b.execute()
	if err != nil {
		return err
	}

	// Store rows affected
	if result != nil {
		// You might want to add this to your DB struct
		// b.db.RowsAffected = result.RowsAffected
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
	b.q.Operation = query.OpUpdate
	b.q.Document = values

	_, err := b.execute()
	return err
}

// Updates modifies records with map
func (b *QueryBuilder) Updates(values map[string]interface{}) error {
	b.q.Operation = query.OpUpdate
	b.q.Updates = values

	_, err := b.execute()
	return err
}

// UpdateColumn updates a single column
func (b *QueryBuilder) UpdateColumn(column string, value interface{}) error {
	return b.Updates(map[string]interface{}{column: value})
}

// Delete removes records
func (b *QueryBuilder) Delete() error {
	b.q.Operation = query.OpDelete

	_, err := b.execute()
	return err
}

// ============ Internal Methods ============

// execute runs the query through the dialect
func (b *QueryBuilder) execute() (*dialect.Result, error) {
	if b.d == nil {
		return nil, fmt.Errorf("no dialect configured")
	}

	return b.d.Execute(b.q.Context, b.q)
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

// parseStringFilter parses string filter like "age > ?"
func (b *QueryBuilder) parseStringFilter(filterStr string, args ...interface{}) *query.Filter {
	parts := strings.Fields(filterStr)
	if len(parts) < 2 {
		return &query.Filter{
			Field:    filterStr,
			Operator: query.OpEqual,
			Value:    args[0],
		}
	}

	field := parts[0]
	operatorStr := strings.ToUpper(parts[1])
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

// ============ Additional Filter Helper Functions ============

// WhereIn adds an IN filter
func (b *QueryBuilder) WhereIn(field string, values ...interface{}) *QueryBuilder {
	return b.Where(&query.Filter{
		Field:    field,
		Operator: query.OpIn,
		Values:   values,
	})
}

// WhereNotIn adds a NOT IN filter
func (b *QueryBuilder) WhereNotIn(field string, values ...interface{}) *QueryBuilder {
	return b.Where(&query.Filter{
		Field:    field,
		Operator: query.OpNotIn,
		Values:   values,
	})
}

// WhereBetween adds a BETWEEN filter
func (b *QueryBuilder) WhereBetween(field string, start, end interface{}) *QueryBuilder {
	return b.Where(&query.Filter{
		Field:        field,
		Operator:     query.OpBetween,
		BetweenStart: start,
		BetweenEnd:   end,
	})
}

// WhereNull adds an IS NULL filter
func (b *QueryBuilder) WhereNull(field string) *QueryBuilder {
	return b.Where(&query.Filter{
		Field:    field,
		Operator: query.OpNull,
	})
}

// WhereNotNull adds an IS NOT NULL filter
func (b *QueryBuilder) WhereNotNull(field string) *QueryBuilder {
	return b.Where(&query.Filter{
		Field:    field,
		Operator: query.OpNotNull,
	})
}

// WhereLike adds a LIKE filter
func (b *QueryBuilder) WhereLike(field, pattern string) *QueryBuilder {
	return b.Where(&query.Filter{
		Field:    field,
		Operator: query.OpLike,
		Value:    pattern,
	})
}

// In adds a generic IN clause (alias for WhereIn)
func (b *QueryBuilder) In(field string, values ...interface{}) *QueryBuilder {
	return b.WhereIn(field, values...)
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
func (b *QueryBuilder) Raw(sql string, args ...interface{}) *QueryBuilder {
	b.q.IsRaw = true
	b.q.Raw = sql
	b.q.RawArgs = args
	return b
}

// Exec executes a query without returning rows
func (b *QueryBuilder) Exec(sql string, args ...interface{}) (int64, error) {
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
