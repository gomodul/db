package translator

import (
	"reflect"
	"strings"
	"unicode"
)

// extractFields returns ordered column names and values from a model (struct or map).
// Struct fields use the "db" tag first, then "json", then snake_cased field name.
// Fields tagged with db:"-" or json:"-" are skipped.
func extractFields(v interface{}) (columns []string, values []interface{}) {
	if v == nil {
		return
	}
	if m, ok := v.(map[string]interface{}); ok {
		for k, val := range m {
			columns = append(columns, k)
			values = append(values, val)
		}
		return
	}
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return
	}
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			continue
		}
		col := fieldColName(field)
		if col == "-" {
			continue
		}
		columns = append(columns, col)
		values = append(values, val.Field(i).Interface())
	}
	return
}

// extractID returns the primary key value from a document (struct or map).
func extractID(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]interface{}); ok {
		for _, key := range []string{"id", "ID", "_id"} {
			if val, exists := m[key]; exists {
				return val
			}
		}
		return nil
	}
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("db")
		if strings.Contains(tag, ",pk") || tag == "pk" {
			return val.Field(i).Interface()
		}
		if field.Name == "ID" || field.Name == "Id" {
			return val.Field(i).Interface()
		}
	}
	return nil
}

// modelCollectionName returns the table/collection name for a model.
// Checks for TableName() method first, then pluralises the snake_cased type name.
func modelCollectionName(v interface{}) string {
	if v == nil {
		return "resource"
	}
	type tabler interface{ TableName() string }
	if t, ok := v.(tabler); ok {
		return t.TableName()
	}
	typ := reflect.TypeOf(v)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return pluralize(toSnakeCase(typ.Name()))
}

// fieldColName returns the column name for a struct field.
func fieldColName(f reflect.StructField) string {
	for _, tag := range []string{"db", "json"} {
		raw := f.Tag.Get(tag)
		if raw == "" {
			continue
		}
		name := strings.SplitN(raw, ",", 2)[0]
		if name == "-" {
			return "-"
		}
		if name != "" {
			return name
		}
	}
	return toSnakeCase(f.Name)
}

// toSnakeCase converts CamelCase to snake_case.
func toSnakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) && i > 0 {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// pluralize appends a simple English plural suffix.
func pluralize(s string) string {
	switch {
	case strings.HasSuffix(s, "y"):
		return s[:len(s)-1] + "ies"
	case strings.HasSuffix(s, "s"), strings.HasSuffix(s, "x"), strings.HasSuffix(s, "z"):
		return s + "es"
	default:
		return s + "s"
	}
}
