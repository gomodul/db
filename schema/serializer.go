package schema

import (
	"context"
	"reflect"
)

// SerializerValuerInterface serializer valuer interface
type SerializerValuerInterface interface {
	Value(ctx context.Context, field *Field, dst reflect.Value, fieldValue interface{}) (interface{}, error)
}

// SerializerInterface serializer interface
type SerializerInterface interface {
	Scan(ctx context.Context, field *Field, dst reflect.Value, dbValue interface{}) error
	SerializerValuerInterface
}
