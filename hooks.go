package db

import (
	"context"
	"fmt"
	"reflect"

	"github.com/gomodul/db/callback"
)

// BeforeCreate is an interface that can be implemented by models to run code before creation.
type BeforeCreate interface {
	BeforeCreate(*callback.Context) error
}

// AfterCreate is an interface that can be implemented by models to run code after creation.
type AfterCreate interface {
	AfterCreate(*callback.Context) error
}

// BeforeUpdate is an interface that can be implemented by models to run code before update.
type BeforeUpdate interface {
	BeforeUpdate(*callback.Context) error
}

// AfterUpdate is an interface that can be implemented by models to run code after update.
type AfterUpdate interface {
	AfterUpdate(*callback.Context) error
}

// BeforeDelete is an interface that can be implemented by models to run code before deletion.
type BeforeDelete interface {
	BeforeDelete(*callback.Context) error
}

// AfterDelete is an interface that can be implemented by models to run code after deletion.
type AfterDelete interface {
	AfterDelete(*callback.Context) error
}

// BeforeFind is an interface that can be implemented by models to run code before query.
type BeforeFind interface {
	BeforeFind(*callback.Context) error
}

// AfterFind is an interface that can be implemented by models to run code after query.
type AfterFind interface {
	AfterFind(*callback.Context) error
}

// HookExecutor executes hooks for models
type HookExecutor struct{}

// NewHookExecutor creates a new hook executor
func NewHookExecutor() *HookExecutor {
	return &HookExecutor{}
}

// BeforeCreate executes BeforeCreate hook if model implements it
func (h *HookExecutor) BeforeCreate(ctx context.Context, model interface{}) error {
	if bc, ok := model.(BeforeCreate); ok {
		callbackCtx := callback.NewContext()
		if err := bc.BeforeCreate(callbackCtx); err != nil {
			return fmt.Errorf("BeforeCreate hook: %w", err)
		}
		if callbackCtx.IsSkipped() {
			return callback.ErrSkipped
		}
	}
	return nil
}

// AfterCreate executes AfterCreate hook if model implements it
func (h *HookExecutor) AfterCreate(ctx context.Context, model interface{}) error {
	if ac, ok := model.(AfterCreate); ok {
		callbackCtx := callback.NewContext()
		if err := ac.AfterCreate(callbackCtx); err != nil {
			return fmt.Errorf("AfterCreate hook: %w", err)
		}
	}
	return nil
}

// BeforeUpdate executes BeforeUpdate hook if model implements it
func (h *HookExecutor) BeforeUpdate(ctx context.Context, model interface{}) error {
	if bu, ok := model.(BeforeUpdate); ok {
		callbackCtx := callback.NewContext()
		if err := bu.BeforeUpdate(callbackCtx); err != nil {
			return fmt.Errorf("BeforeUpdate hook: %w", err)
		}
		if callbackCtx.IsSkipped() {
			return callback.ErrSkipped
		}
	}
	return nil
}

// AfterUpdate executes AfterUpdate hook if model implements it
func (h *HookExecutor) AfterUpdate(ctx context.Context, model interface{}) error {
	if au, ok := model.(AfterUpdate); ok {
		callbackCtx := callback.NewContext()
		if err := au.AfterUpdate(callbackCtx); err != nil {
			return fmt.Errorf("AfterUpdate hook: %w", err)
		}
	}
	return nil
}

// BeforeDelete executes BeforeDelete hook if model implements it
func (h *HookExecutor) BeforeDelete(ctx context.Context, model interface{}) error {
	if bd, ok := model.(BeforeDelete); ok {
		callbackCtx := callback.NewContext()
		if err := bd.BeforeDelete(callbackCtx); err != nil {
			return fmt.Errorf("BeforeDelete hook: %w", err)
		}
		if callbackCtx.IsSkipped() {
			return callback.ErrSkipped
		}
	}
	return nil
}

// AfterDelete executes AfterDelete hook if model implements it
func (h *HookExecutor) AfterDelete(ctx context.Context, model interface{}) error {
	if ad, ok := model.(AfterDelete); ok {
		callbackCtx := callback.NewContext()
		if err := ad.AfterDelete(callbackCtx); err != nil {
			return fmt.Errorf("AfterDelete hook: %w", err)
		}
	}
	return nil
}

// BeforeFind executes BeforeFind hook if model implements it
func (h *HookExecutor) BeforeFind(ctx context.Context, model interface{}) error {
	if bf, ok := model.(BeforeFind); ok {
		callbackCtx := callback.NewContext()
		if err := bf.BeforeFind(callbackCtx); err != nil {
			return fmt.Errorf("BeforeFind hook: %w", err)
		}
		if callbackCtx.IsSkipped() {
			return callback.ErrSkipped
		}
	}
	return nil
}

// AfterFind executes AfterFind hook if model implements it
func (h *HookExecutor) AfterFind(ctx context.Context, model interface{}) error {
	if af, ok := model.(AfterFind); ok {
		callbackCtx := callback.NewContext()
		if err := af.AfterFind(callbackCtx); err != nil {
			return fmt.Errorf("AfterFind hook: %w", err)
		}
	}
	return nil
}

// ExecuteHooks executes hooks for slice of models
func (h *HookExecutor) ExecuteHooks(ctx context.Context, operation string, models interface{}) error {
	rv := reflect.ValueOf(models)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return h.executeHook(ctx, operation, models)
	}

	for i := 0; i < rv.Len(); i++ {
		model := rv.Index(i).Interface()
		if err := h.executeHook(ctx, operation, model); err != nil {
			return err
		}
	}
	return nil
}

// executeHook executes a single hook based on operation type
func (h *HookExecutor) executeHook(ctx context.Context, operation string, model interface{}) error {
	switch operation {
	case "before_create":
		return h.BeforeCreate(ctx, model)
	case "after_create":
		return h.AfterCreate(ctx, model)
	case "before_update":
		return h.BeforeUpdate(ctx, model)
	case "after_update":
		return h.AfterUpdate(ctx, model)
	case "before_delete":
		return h.BeforeDelete(ctx, model)
	case "after_delete":
		return h.AfterDelete(ctx, model)
	case "before_find":
		return h.BeforeFind(ctx, model)
	case "after_find":
		return h.AfterFind(ctx, model)
	default:
		return fmt.Errorf("unknown operation: %s", operation)
	}
}
