package plugin

import (
	"context"
	"fmt"

	"github.com/gomodul/db"
)

// Plugin is the interface that all plugins must implement
type Plugin interface {
	// Name returns the unique name of the plugin
	Name() string

	// Version returns the version of the plugin
	Version() string

	// Init initializes the plugin with the database instance
	Init(database *db.DB) error

	// Close cleans up plugin resources
	Close() error
}

// HookPlugin extends Plugin with hooks for database operations
type HookPlugin interface {
	Plugin

	// BeforeCreate is called before a create operation
	BeforeCreate(ctx context.Context, model interface{}) error

	// AfterCreate is called after a create operation
	AfterCreate(ctx context.Context, model interface{}) error

	// BeforeUpdate is called before an update operation
	BeforeUpdate(ctx context.Context, model interface{}) error

	// AfterUpdate is called after an update operation
	AfterUpdate(ctx context.Context, model interface{}) error

	// BeforeDelete is called before a delete operation
	BeforeDelete(ctx context.Context, model interface{}) error

	// AfterDelete is called after a delete operation
	AfterDelete(ctx context.Context, model interface{}) error

	// BeforeQuery is called before a query operation
	BeforeQuery(ctx context.Context, query string, args ...interface{}) error

	// AfterQuery is called after a query operation
	AfterQuery(ctx context.Context, result interface{}) error
}

// QueryPlugin extends Plugin with query interception
type QueryPlugin interface {
	Plugin

	// InterceptQuery is called before executing a query
	// Can modify the query or args, or return a custom result
	InterceptQuery(ctx context.Context, query string, args ...interface{}) (string, []interface{}, error)

	// InterceptResult is called after executing a query
	// Can modify the result before it's returned
	InterceptResult(ctx context.Context, result interface{}) error
}

// TransactionPlugin extends Plugin with transaction hooks
type TransactionPlugin interface {
	Plugin

	// BeforeBegin is called before starting a transaction
	BeforeBegin(ctx context.Context) error

	// AfterBegin is called after starting a transaction
	AfterBegin(ctx context.Context, txID string) error

	// BeforeCommit is called before committing a transaction
	BeforeCommit(ctx context.Context, txID string) error

	// AfterCommit is called after committing a transaction
	AfterCommit(ctx context.Context, txID string) error

	// BeforeRollback is called before rolling back a transaction
	BeforeRollback(ctx context.Context, txID string) error

	// AfterRollback is called after rolling back a transaction
	AfterRollback(ctx context.Context, txID string) error
}

// ConnectionPlugin extends Plugin with connection management
type ConnectionPlugin interface {
	Plugin

	// OnConnect is called when a new connection is established
	OnConnect(ctx context.Context) error

	// OnDisconnect is called when a connection is closed
	OnDisconnect(ctx context.Context) error

	// OnConnectionError is called when a connection error occurs
	OnConnectionError(ctx context.Context, err error)
}

// ValidationPlugin extends Plugin with custom validation
type ValidationPlugin interface {
	Plugin

	// ValidateModel is called before create/update operations
	ValidateModel(ctx context.Context, model interface{}) error

	// ValidateField is called for each field during validation
	ValidateField(ctx context.Context, model interface{}, field string, value interface{}) error
}

// PluginInfo contains information about a plugin
type PluginInfo struct {
	Name        string
	Version     string
	Description string
	Author      string
	Plugin      Plugin
}

// PluginError represents an error from a plugin
type PluginError struct {
	PluginName string
	Err        error
}

func (e *PluginError) Error() string {
	return fmt.Sprintf("plugin %s error: %v", e.PluginName, e.Err)
}

func (e *PluginError) Unwrap() error {
	return e.Err
}

// WrapError wraps an error with plugin information
func WrapError(pluginName string, err error) error {
	if err == nil {
		return nil
	}
	return &PluginError{
		PluginName: pluginName,
		Err:        err,
	}
}
