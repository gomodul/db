package callback

import (
	"fmt"
	"reflect"
	"sync"
)

// Callback is a struct that manages callback functions for database operations
type Callback struct {
	creates  *Processor
	updates  *Processor
	queries  *Processor
	deletes  *Processor
	row      *Processor
	raw      *Processor

	mu sync.RWMutex
}

// Processor manages callback functions for a specific operation
type Processor struct {
	name      string
	callbacks []*callback
	mu        sync.RWMutex
}

// callback represents a single callback function
type callback struct {
	name      string
	handler   interface{}
	before    string // after which callback
	replace   bool
	removed   bool
}

// CallbackFunc is the interface for callback functions
type CallbackFunc func(*Context)

// Create returns the processor for create operations
func (c *Callback) Create() *Processor {
	if c.creates == nil {
		c.creates = &Processor{name: "create"}
	}
	return c.creates
}

// Update returns the processor for update operations
func (c *Callback) Update() *Processor {
	if c.updates == nil {
		c.updates = &Processor{name: "update"}
	}
	return c.updates
}

// Query returns the processor for query operations
func (c *Callback) Query() *Processor {
	if c.queries == nil {
		c.queries = &Processor{name: "query"}
	}
	return c.queries
}

// Delete returns the processor for delete operations
func (c *Callback) Delete() *Processor {
	if c.deletes == nil {
		c.deletes = &Processor{name: "delete"}
	}
	return c.deletes
}

// Row returns the processor for row operations
func (c *Callback) Row() *Processor {
	if c.row == nil {
		c.row = &Processor{name: "row"}
	}
	return c.row
}

// Raw returns the processor for raw query operations
func (c *Callback) Raw() *Processor {
	if c.raw == nil {
		c.raw = &Processor{name: "raw"}
	}
	return c.raw
}

// Register registers a new callback with the given name
func (p *Processor) Register(name string, handler interface{}) *Processor {
	return p.addCallback(name, handler, "", false, false)
}

// RegisterBefore registers a callback to be executed before another callback
func (p *Processor) RegisterBefore(name string, handler interface{}, before string) *Processor {
	return p.addCallback(name, handler, before, false, false)
}

// RegisterAfter registers a callback to be executed after another callback
func (p *Processor) RegisterAfter(name string, handler interface{}, after string) *Processor {
	return p.addCallback(name, handler, after, false, false)
}

// Replace replaces a callback with the given name
func (p *Processor) Replace(name string, handler interface{}) *Processor {
	return p.addCallback(name, handler, "", true, false)
}

// Remove removes a callback with the given name
func (p *Processor) Remove(name string) *Processor {
	return p.addCallback(name, nil, "", false, true)
}

// Get returns all callbacks with the given name
func (p *Processor) Get(name string) []interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []interface{}
	for _, cb := range p.callbacks {
		if cb.name == name && !cb.removed {
			result = append(result, cb.handler)
		}
	}
	return result
}

// addCallback adds a callback to the processor
func (p *Processor) addCallback(name string, handler interface{}, before string, replace, removed bool) *Processor {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if callback already exists
	for i, cb := range p.callbacks {
		if cb.name == name {
			if replace {
				// Mark existing callback as removed
				p.callbacks[i].removed = true
			} else if removed {
				p.callbacks[i].removed = true
				return p
			} else {
				return p // Already exists
			}
		}
	}

	newCallback := &callback{
		name:    name,
		handler: handler,
		before:  before,
		removed: removed,
	}

	if before != "" {
		// Find position to insert before the specified callback
		for i, cb := range p.callbacks {
			if cb.name == before {
				// Insert before this callback
				p.callbacks = append(p.callbacks[:i], append([]*callback{newCallback}, p.callbacks[i:]...)...)
				return p
			}
		}
	}

	p.callbacks = append(p.callbacks, newCallback)
	return p
}

// Execute executes all non-removed callbacks for this processor
func (p *Processor) Execute(dest interface{}, ctx *Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if ctx == nil {
		ctx = &Context{}
	}

	// Set dest in context if not set
	if ctx.dest == nil {
		ctx.dest = dest
	}

	for _, cb := range p.callbacks {
		if cb.removed {
			continue
		}

		if cb.handler == nil {
			continue
		}

		// Execute the callback
		if err := p.executeCallback(cb.handler, dest, ctx); err != nil {
			return err
		}

		// Check if the operation was skipped
		if ctx.skipped {
			return ErrSkipped
		}
	}

	return nil
}

// executeCallback executes a single callback function
func (p *Processor) executeCallback(handler interface{}, dest interface{}, ctx *Context) error {
	handlerValue := reflect.ValueOf(handler)
	handlerType := handlerValue.Type()

	// Get the dest value
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() == reflect.Ptr {
		destValue = destValue.Elem()
	}

	// Handle different function signatures
	switch handlerType.NumIn() {
	case 1:
		// func(*DB) or func(*Model)
		arg := handlerType.In(0)
		if arg == reflect.TypeOf(ctx) {
			results := handlerValue.Call([]reflect.Value{reflect.ValueOf(ctx)})
			return p.getError(results)
		}
		results := handlerValue.Call([]reflect.Value{reflect.ValueOf(dest)})
		return p.getError(results)

	case 2:
		// func(*DB, *Context)
		results := handlerValue.Call([]reflect.Value{reflect.ValueOf(dest), reflect.ValueOf(ctx)})
		return p.getError(results)

	default:
		return fmt.Errorf("invalid callback signature: %v", handlerType)
	}
}

// getError extracts error from callback result
func (p *Processor) getError(results []reflect.Value) error {
	if len(results) == 0 {
		return nil
	}

	lastResult := results[len(results)-1]
	if !lastResult.IsValid() || lastResult.IsNil() {
		return nil
	}

	if err, ok := lastResult.Interface().(error); ok {
		return err
	}

	return nil
}

// Context is passed to callback functions
type Context struct {
	dest        interface{}
	statement   interface{}
	skipped     bool
	skipLeft    bool
	query       string
	args        []interface{}
	result      interface{}
	error       error
	Metadata    map[string]interface{}
	mu          sync.RWMutex
}

// NewContext creates a new callback context
func NewContext() *Context {
	return &Context{
		Metadata: make(map[string]interface{}),
	}
}

// Skip skips the operation
func (c *Context) Skip() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.skipped = true
}

// IsSkipped returns true if the operation was skipped
func (c *Context) IsSkipped() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.skipped
}

// SetStatement sets the statement for this context
func (c *Context) SetStatement(stmt interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statement = stmt
}

// Statement returns the statement for this context
func (c *Context) Statement() interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.statement
}

// SetQuery sets the SQL query for this context
func (c *Context) SetQuery(query string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.query = query
}

// Query returns the SQL query for this context
func (c *Context) Query() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.query
}

// SetArgs sets the query arguments for this context
func (c *Context) SetArgs(args []interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.args = args
}

// Args returns the query arguments for this context
func (c *Context) Args() []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.args
}

// SetResult sets the result for this context
func (c *Context) SetResult(result interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.result = result
}

// Result returns the result for this context
func (c *Context) Result() interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.result
}

// SetError sets the error for this context
func (c *Context) SetError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.error = err
}

// Error returns the error for this context
func (c *Context) Error() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.error
}

// Set sets a metadata value
func (c *Context) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Metadata == nil {
		c.Metadata = make(map[string]interface{})
	}
	c.Metadata[key] = value
}

// Get gets a metadata value
func (c *Context) Get(key string) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.Metadata == nil {
		return nil
	}
	return c.Metadata[key]
}

// Errors
var (
	// ErrSkipped is returned when a callback skips the operation
	ErrSkipped = fmt.Errorf("callback skipped operation")
)
