package callback

import (
	"fmt"
	"reflect"
	"sync"
)

// ProcessorFunc is a function that processes a callback
type ProcessorFunc func(*Context) error

// CreateProcessor handles callbacks for create operations
type CreateProcessor struct {
	callbacks map[string]*callback
	mu        sync.RWMutex
}

// NewCreateProcessor creates a new create processor
func NewCreateProcessor() *CreateProcessor {
	return &CreateProcessor{
		callbacks: make(map[string]*callback),
	}
}

// BeforeCreate executes callbacks before create
func (p *CreateProcessor) BeforeCreate(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("before_create", dest, ctx)
}

// AfterCreate executes callbacks after create
func (p *CreateProcessor) AfterCreate(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("after_create", dest, ctx)
}

// Register registers a callback for a specific event
func (p *CreateProcessor) Register(event string, handler interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.callbacks[event] = &callback{
		name:    event,
		handler: handler,
	}
	return nil
}

// executeCallbacks executes callbacks for a specific event
func (p *CreateProcessor) executeCallbacks(event string, dest interface{}, ctx *Context) error {
	p.mu.RLock()
	cb, exists := p.callbacks[event]
	p.mu.RUnlock()

	if !exists {
		return nil
	}

	if cb.removed {
		return nil
	}

	handlerValue := reflect.ValueOf(cb.handler)
	if !handlerValue.IsValid() {
		return nil
	}

	// Call the callback
	results := handlerValue.Call([]reflect.Value{
		reflect.ValueOf(dest),
		reflect.ValueOf(ctx),
	})

	if len(results) > 0 {
		if err, ok := results[0].Interface().(error); ok && err != nil {
			return err
		}
	}

	return nil
}

// UpdateProcessor handles callbacks for update operations
type UpdateProcessor struct {
	callbacks map[string]*callback
	mu        sync.RWMutex
}

// NewUpdateProcessor creates a new update processor
func NewUpdateProcessor() *UpdateProcessor {
	return &UpdateProcessor{
		callbacks: make(map[string]*callback),
	}
}

// BeforeUpdate executes callbacks before update
func (p *UpdateProcessor) BeforeUpdate(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("before_update", dest, ctx)
}

// AfterUpdate executes callbacks after update
func (p *UpdateProcessor) AfterUpdate(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("after_update", dest, ctx)
}

// Register registers a callback for a specific event
func (p *UpdateProcessor) Register(event string, handler interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.callbacks[event] = &callback{
		name:    event,
		handler: handler,
	}
	return nil
}

// executeCallbacks executes callbacks for a specific event
func (p *UpdateProcessor) executeCallbacks(event string, dest interface{}, ctx *Context) error {
	p.mu.RLock()
	cb, exists := p.callbacks[event]
	p.mu.RUnlock()

	if !exists {
		return nil
	}

	if cb.removed {
		return nil
	}

	handlerValue := reflect.ValueOf(cb.handler)
	if !handlerValue.IsValid() {
		return nil
	}

	results := handlerValue.Call([]reflect.Value{
		reflect.ValueOf(dest),
		reflect.ValueOf(ctx),
	})

	if len(results) > 0 {
		if err, ok := results[0].Interface().(error); ok && err != nil {
			return err
		}
	}

	return nil
}

// DeleteProcessor handles callbacks for delete operations
type DeleteProcessor struct {
	callbacks map[string]*callback
	mu        sync.RWMutex
}

// NewDeleteProcessor creates a new delete processor
func NewDeleteProcessor() *DeleteProcessor {
	return &DeleteProcessor{
		callbacks: make(map[string]*callback),
	}
}

// BeforeDelete executes callbacks before delete
func (p *DeleteProcessor) BeforeDelete(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("before_delete", dest, ctx)
}

// AfterDelete executes callbacks after delete
func (p *DeleteProcessor) AfterDelete(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("after_delete", dest, ctx)
}

// Register registers a callback for a specific event
func (p *DeleteProcessor) Register(event string, handler interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.callbacks[event] = &callback{
		name:    event,
		handler: handler,
	}
	return nil
}

// executeCallbacks executes callbacks for a specific event
func (p *DeleteProcessor) executeCallbacks(event string, dest interface{}, ctx *Context) error {
	p.mu.RLock()
	cb, exists := p.callbacks[event]
	p.mu.RUnlock()

	if !exists {
		return nil
	}

	if cb.removed {
		return nil
	}

	handlerValue := reflect.ValueOf(cb.handler)
	if !handlerValue.IsValid() {
		return nil
	}

	results := handlerValue.Call([]reflect.Value{
		reflect.ValueOf(dest),
		reflect.ValueOf(ctx),
	})

	if len(results) > 0 {
		if err, ok := results[0].Interface().(error); ok && err != nil {
			return err
		}
	}

	return nil
}

// QueryProcessor handles callbacks for query operations
type QueryProcessor struct {
	callbacks map[string]*callback
	mu        sync.RWMutex
}

// NewQueryProcessor creates a new query processor
func NewQueryProcessor() *QueryProcessor {
	return &QueryProcessor{
		callbacks: make(map[string]*callback),
	}
}

// BeforeQuery executes callbacks before query
func (p *QueryProcessor) BeforeQuery(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("before_query", dest, ctx)
}

// AfterQuery executes callbacks after query
func (p *QueryProcessor) AfterQuery(dest interface{}, ctx *Context) error {
	return p.executeCallbacks("after_query", dest, ctx)
}

// Register registers a callback for a specific event
func (p *QueryProcessor) Register(event string, handler interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.callbacks[event] = &callback{
		name:    event,
		handler: handler,
	}
	return nil
}

// executeCallbacks executes callbacks for a specific event
func (p *QueryProcessor) executeCallbacks(event string, dest interface{}, ctx *Context) error {
	p.mu.RLock()
	cb, exists := p.callbacks[event]
	p.mu.RUnlock()

	if !exists {
		return nil
	}

	if cb.removed {
		return nil
	}

	handlerValue := reflect.ValueOf(cb.handler)
	if !handlerValue.IsValid() {
		return nil
	}

	results := handlerValue.Call([]reflect.Value{
		reflect.ValueOf(dest),
		reflect.ValueOf(ctx),
	})

	if len(results) > 0 {
		if err, ok := results[0].Interface().(error); ok && err != nil {
			return err
		}
	}

	return nil
}

// CallbackRegistry holds all callback processors
type CallbackRegistry struct {
	create *CreateProcessor
	update *UpdateProcessor
	delete *DeleteProcessor
	query  *QueryProcessor
	mu     sync.RWMutex
}

// NewCallbackRegistry creates a new callback registry
func NewCallbackRegistry() *CallbackRegistry {
	return &CallbackRegistry{
		create: NewCreateProcessor(),
		update: NewUpdateProcessor(),
		delete: NewDeleteProcessor(),
		query:  NewQueryProcessor(),
	}
}

// Create returns the create processor
func (r *CallbackRegistry) Create() *CreateProcessor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.create
}

// Update returns the update processor
func (r *CallbackRegistry) Update() *UpdateProcessor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.update
}

// Delete returns the delete processor
func (r *CallbackRegistry) Delete() *DeleteProcessor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.delete
}

// Query returns the query processor
func (r *CallbackRegistry) Query() *QueryProcessor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.query
}

// ExecuteInterfaceCallbacks executes callbacks from interface implementations
func ExecuteInterfaceCallbacks(operation string, dest interface{}, ctx *Context) error {
	ctx = ensureContext(ctx)

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("destination must be a pointer")
	}

	// Dereference pointer
	elem := rv.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("destination must point to a struct")
	}

	// Check for interface implementations
	switch operation {
	case "create":
		if bc, ok := dest.(interface{ BeforeCreate(*Context) error }); ok {
			if err := bc.BeforeCreate(ctx); err != nil {
				return err
			}
		}
		if ac, ok := dest.(interface{ AfterCreate(*Context) error }); ok {
			if err := ac.AfterCreate(ctx); err != nil {
				return err
			}
		}
	case "update":
		if bu, ok := dest.(interface{ BeforeUpdate(*Context) error }); ok {
			if err := bu.BeforeUpdate(ctx); err != nil {
				return err
			}
		}
		if au, ok := dest.(interface{ AfterUpdate(*Context) error }); ok {
			if err := au.AfterUpdate(ctx); err != nil {
				return err
			}
		}
	case "delete":
		if bd, ok := dest.(interface{ BeforeDelete(*Context) error }); ok {
			if err := bd.BeforeDelete(ctx); err != nil {
				return err
			}
		}
		if ad, ok := dest.(interface{ AfterDelete(*Context) error }); ok {
			if err := ad.AfterDelete(ctx); err != nil {
				return err
			}
		}
	case "query":
		if bq, ok := dest.(interface{ BeforeQuery(*Context) error }); ok {
			if err := bq.BeforeQuery(ctx); err != nil {
				return err
			}
		}
		if aq, ok := dest.(interface{ AfterQuery(*Context) error }); ok {
			if err := aq.AfterQuery(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

// ensureContext ensures the context is not nil
func ensureContext(ctx *Context) *Context {
	if ctx == nil {
		ctx = &Context{}
	}
	return ctx
}

