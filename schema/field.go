package schema

import (
	"context"
	"reflect"
)

// FieldNewValuePool field new scan value pool
type FieldNewValuePool interface {
	Get() interface{}
	Put(interface{})
}

// Field is the representation of model schema's field
type Field struct {
	Name                   string
	DBName                 string
	BindNames              []string
	EmbeddedBindNames      []string
	DataType               DataType
	GORMDataType           DataType
	PrimaryKey             bool
	AutoIncrement          bool
	AutoIncrementIncrement int64
	Creatable              bool
	Updatable              bool
	Readable               bool
	AutoCreateTime         TimeType
	AutoUpdateTime         TimeType
	HasDefaultValue        bool
	DefaultValue           string
	DefaultValueInterface  interface{}
	NotNull                bool
	Unique                 bool
	Comment                string
	Size                   int
	Precision              int
	Scale                  int
	IgnoreMigration        bool
	FieldType              reflect.Type
	IndirectFieldType      reflect.Type
	StructField            reflect.StructField
	Tag                    reflect.StructTag
	TagSettings            map[string]string
	Schema                 *Schema
	EmbeddedSchema         *Schema
	OwnerSchema            *Schema
	ReflectValueOf         func(context.Context, reflect.Value) reflect.Value
	ValueOf                func(context.Context, reflect.Value) (value interface{}, zero bool)
	Set                    func(context.Context, reflect.Value, interface{}) error
	Serializer             SerializerInterface
	NewValuePool           FieldNewValuePool

	// In some db (e.g. MySQL), Unique and UniqueIndex are indistinguishable.
	// When a column has a (not Mul) UniqueIndex, Migrator always reports its gorm.ColumnType is Unique.
	// It causes field unnecessarily migration.
	// Therefore, we need to record the UniqueIndex on this column (exclude Mul UniqueIndex) for MigrateColumnUnique.
	UniqueIndex string
}
