package builder

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gomodul/db/query"
)

// ============ Update Operations ============

// UpdateColumnExpr updates a column with an expression
func (b *QueryBuilder) UpdateColumnExpr(column, expr string) *QueryBuilder {
	if b.q.Updates == nil {
		b.q.Updates = make(map[string]interface{})
	}
	// Store expression as a special marker
	b.q.Updates[column] = map[string]interface{}{
		"_expr": expr,
	}
	return b
}

// Increment increments a column by value.
func (b *QueryBuilder) Increment(column string, value interface{}) *QueryBuilder {
	return b.UpdateColumnExpr(column, fmt.Sprintf("%s + %v", column, value))
}

// Decrement decrements a column by value.
func (b *QueryBuilder) Decrement(column string, value interface{}) *QueryBuilder {
	return b.UpdateColumnExpr(column, fmt.Sprintf("%s - %v", column, value))
}

// ============ Condition-Based Updates ============

// UpdateWhere updates records matching the where condition
func (b *QueryBuilder) UpdateWhere(updates map[string]interface{}, filter interface{}, args ...interface{}) error {
	b.Where(filter, args...)
	b.q.Updates = updates
	b.q.Operation = query.OpUpdate

	result, err := b.execute()
	if err != nil {
		return err
	}

	b.db.RowsAffected = result.RowsAffected
	return nil
}

// UpdateColumnWhere updates a column for records matching the condition
func (b *QueryBuilder) UpdateColumnWhere(column string, value interface{}, filter interface{}, args ...interface{}) error {
	return b.UpdateWhere(map[string]interface{}{
		column: value,
	}, filter, args...)
}

// IncrementWhere increments a column for matching records
func (b *QueryBuilder) IncrementWhere(column string, value int, filter interface{}, args ...interface{}) error {
	return b.UpdateColumnWhere(column, value, filter, args...)
}

// DecrementWhere decrements a column for matching records
func (b *QueryBuilder) DecrementWhere(column string, value int, filter interface{}, args ...interface{}) error {
	return b.UpdateColumnWhere(column, -value, filter, args...)
}

// ============ Upsert Operations ============

// OnConflict sets the ON CONFLICT behavior (SQL databases with UPSERT support)
func (b *QueryBuilder) OnConflict(targets ...string) *OnConflict {
	return &OnConflict{
		b:       b,
		targets: targets,
	}
}

// OnConflictIgnore is shorthand for OnConflict().DoNothing()
func (b *QueryBuilder) OnConflictIgnore() *QueryBuilder {
	if !b.db.Caps.HasFeature("upsert") {
		b.db.Error = ErrNotSupported
		return b
	}

	b.q.Hints = map[string]interface{}{
		"on_conflict": "ignore",
	}
	return b
}

// OnConflictUpdate is shorthand for OnConflict().DoUpdates()
func (b *QueryBuilder) OnConflictUpdate(columns ...string) *QueryBuilder {
	if !b.db.Caps.HasFeature("upsert") {
		b.db.Error = ErrNotSupported
		return b
	}

	b.q.Hints = map[string]interface{}{
		"on_conflict":    "update",
		"update_columns": columns,
	}
	return b
}

// OnConflict represents an ON CONFLICT clause
type OnConflict struct {
	b       *QueryBuilder
	targets []string
}

// DoNothing sets the ON CONFLICT action to do nothing
func (oc *OnConflict) DoNothing() *QueryBuilder {
	oc.b.q.Hints = map[string]interface{}{
		"on_conflict": "do_nothing",
	}
	return oc.b
}

// DoUpdate sets the ON CONFLICT action to update
func (oc *OnConflict) DoUpdate(columns ...string) *QueryBuilder {
	oc.b.q.Hints = map[string]interface{}{
		"on_conflict":    "update",
		"update_columns": columns,
	}
	return oc.b
}

// Update sets the update values
func (oc *OnConflict) Update(updates map[string]interface{}) *QueryBuilder {
	oc.b.q.Updates = updates
	oc.b.q.Hints = map[string]interface{}{
		"on_conflict": "update",
	}
	return oc.b
}


// ============ Bulk Operations ============

// CreateEach creates multiple records one by one
func (b *QueryBuilder) CreateEach(values interface{}) error {
	vals := reflect.ValueOf(values)
	if vals.Kind() != reflect.Slice && vals.Kind() != reflect.Array {
		return fmt.Errorf("values must be a slice or array")
	}

	var totalAffected int64
	for i := 0; i < vals.Len(); i++ {
		err := b.Create(vals.Index(i).Interface())
		if err != nil {
			return err
		}
		totalAffected += b.db.RowsAffected
	}

	b.db.RowsAffected = totalAffected
	return nil
}

// SaveEach saves multiple records (create or update each one)
func (b *QueryBuilder) SaveEach(values interface{}) error {
	vals := reflect.ValueOf(values)
	if vals.Kind() != reflect.Slice && vals.Kind() != reflect.Array {
		return fmt.Errorf("values must be a slice or array")
	}

	var totalAffected int64
	for i := 0; i < vals.Len(); i++ {
		item := vals.Index(i).Interface()

		// Check if has primary key
		if b.hasPrimaryKey(item) {
			err := b.Update(item)
			if err != nil {
				return err
			}
		} else {
			err := b.Create(item)
			if err != nil {
				return err
			}
		}

		totalAffected += b.db.RowsAffected
	}

	b.db.RowsAffected = totalAffected
	return nil
}

// UpdateEach updates multiple records one by one
func (b *QueryBuilder) UpdateEach(values interface{}) error {
	vals := reflect.ValueOf(values)
	if vals.Kind() != reflect.Slice && vals.Kind() != reflect.Array {
		return fmt.Errorf("values must be a slice or array")
	}

	var totalAffected int64
	for i := 0; i < vals.Len(); i++ {
		item := vals.Index(i).Interface()

		// Extract primary key for WHERE clause
		pkField, pkValue := b.extractPrimaryKey(item)
		if pkField == "" {
			return fmt.Errorf("record %d has no primary key", i)
		}

		err := b.Where(pkField+" = ?", pkValue).Update(item)
		if err != nil {
			return err
		}

		totalAffected += b.db.RowsAffected
	}

	b.db.RowsAffected = totalAffected
	return nil
}

// DeleteEach deletes multiple records by their primary keys
func (b *QueryBuilder) DeleteEach(keys ...interface{}) error {
	pkField := b.getPrimaryKeyField()
	if pkField == "" {
		return fmt.Errorf("model has no primary key field")
	}

	return b.WhereIn(pkField, keys...).Delete()
}

// ============ Internal Update Helpers ============

// extractPrimaryKey extracts the primary key field and value from a model
func (b *QueryBuilder) extractPrimaryKey(model interface{}) (string, interface{}) {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return "", nil
	}

	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("db")

		// Check for pk tag
		if strings.Contains(tag, ",pk") || tag == "pk" {
			fieldVal := val.Field(i)
			return getFieldName(field), fieldVal.Interface()
		}

		// Check for ID field
		if field.Name == "ID" || field.Name == "Id" {
			fieldVal := val.Field(i)
			return getFieldName(field), fieldVal.Interface()
		}
	}

	return "", nil
}

// getPrimaryKeyField returns the primary key field name
func (b *QueryBuilder) getPrimaryKeyField() string {
	if b.q.Model == nil {
		return ""
	}

	val := reflect.ValueOf(b.q.Model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return ""
	}

	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("db")

		if strings.Contains(tag, ",pk") || strings.Contains(tag, "primarykey") || tag == "pk" {
			return getFieldName(field)
		}

		if field.Name == "ID" || field.Name == "Id" {
			return getFieldName(field)
		}
	}

	return ""
}

// ============ Soft Delete Methods ============

// SoftDelete performs a soft delete (sets deleted_at to current time).
func (b *QueryBuilder) SoftDelete() error {
	if !b.hasDeletedAtField() {
		return ErrNotSupported
	}

	return b.Updates(map[string]interface{}{
		"deleted_at": time.Now(),
	})
}

// Unscoped disables soft delete scope
func (b *QueryBuilder) Unscoped() *QueryBuilder {
	if b.q.Hints == nil {
		b.q.Hints = make(map[string]interface{})
	}
	b.q.Hints["unscoped"] = true
	return b
}

// OnlyDeleted returns only deleted records
func (b *QueryBuilder) OnlyDeleted() *QueryBuilder {
	if b.hasDeletedAtField() {
		b.WhereNotNull("deleted_at")
	}
	return b
}

// WithoutDeleted returns only non-deleted records (default behavior)
func (b *QueryBuilder) WithoutDeleted() *QueryBuilder {
	if b.hasDeletedAtField() {
		b.WhereNull("deleted_at")
	}
	return b
}

// isUnscoped reports whether the query was explicitly marked to include deleted records.
func (b *QueryBuilder) isUnscoped() bool {
	if b.q.Hints == nil {
		return false
	}
	v, ok := b.q.Hints["unscoped"]
	if !ok {
		return false
	}
	unscoped, _ := v.(bool)
	return unscoped
}

// hasDeletedAtField checks if the model has a deleted_at field
func (b *QueryBuilder) hasDeletedAtField() bool {
	if b.q.Model == nil {
		return false
	}

	val := reflect.ValueOf(b.q.Model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return false
	}

	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := getFieldName(field)
		if name == "deleted_at" {
			return true
		}
	}

	return false
}

// ============ Timestamp Methods ============

// UpdatedAt sets the updated_at field
func (b *QueryBuilder) UpdatedAt() *QueryBuilder {
	if b.hasTimestampFields() {
		now := b.q.Context.Value("now")
		if now == nil {
			now = time.Now()
		}
		b.UpdateColumn("updated_at", now)
	}
	return b
}

// CreatedAt sets the created_at field
func (b *QueryBuilder) CreatedAt() *QueryBuilder {
	if b.hasTimestampFields() {
		now := b.q.Context.Value("now")
		if now == nil {
			now = time.Now()
		}
		b.UpdateColumn("created_at", now)
	}
	return b
}

// Touch updates the updated_at timestamp
func (b *QueryBuilder) Touch() error {
	if b.hasTimestampFields() {
		now := b.q.Context.Value("now")
		if now == nil {
			now = time.Now()
		}

		// Need to get primary key first
		pkField, pkValue := b.extractPrimaryKey(b.q.Model)
		if pkField == "" {
			return fmt.Errorf("cannot Touch without primary key")
		}

		return b.Where(pkField+" = ?", pkValue).UpdateColumn("updated_at", now)
	}
	return nil
}

// hasTimestampFields checks if model has timestamp fields
func (b *QueryBuilder) hasTimestampFields() bool {
	if b.q.Model == nil {
		return false
	}

	val := reflect.ValueOf(b.q.Model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return false
	}

	typ := val.Type()
	fields := make(map[string]bool)
	for i := 0; i < typ.NumField(); i++ {
		name := getFieldName(typ.Field(i))
		fields[name] = true
	}

	return fields["created_at"] || fields["updated_at"] || fields["deleted_at"]
}
