package plugin

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/gomodul/db"
)

// Registry manages all registered plugins
type Registry struct {
	mu      sync.RWMutex
	plugins map[string]*PluginInfo
	db      *db.DB
}

// NewRegistry creates a new plugin registry
func NewRegistry(database *db.DB) *Registry {
	return &Registry{
		plugins: make(map[string]*PluginInfo),
		db:      database,
	}
}

// Register registers a plugin
func (r *Registry) Register(plugin Plugin) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := strings.ToLower(plugin.Name())
	if _, ok := r.plugins[name]; ok {
		return fmt.Errorf("plugin %s already registered", name)
	}

	// Initialize the plugin
	if err := plugin.Init(r.db); err != nil {
		return WrapError(plugin.Name(), fmt.Errorf("failed to initialize: %w", err))
	}

	r.plugins[name] = &PluginInfo{
		Name:  plugin.Name(),
		Plugin: plugin,
	}

	return nil
}

// Unregister removes a plugin
func (r *Registry) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name = strings.ToLower(name)
	info, ok := r.plugins[name]
	if !ok {
		return fmt.Errorf("plugin %s not found", name)
	}

	// Close the plugin
	if err := info.Plugin.Close(); err != nil {
		return WrapError(info.Name, fmt.Errorf("failed to close: %w", err))
	}

	delete(r.plugins, name)
	return nil
}

// Get retrieves a plugin by name
func (r *Registry) Get(name string) (Plugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name = strings.ToLower(name)
	info, ok := r.plugins[name]
	if !ok {
		return nil, false
	}
	return info.Plugin, true
}

// List returns all registered plugin names
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.plugins))
	for _, info := range r.plugins {
		names = append(names, info.Name)
	}
	return names
}

// ListInfo returns information about all registered plugins
func (r *Registry) ListInfo() []*PluginInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	infos := make([]*PluginInfo, 0, len(r.plugins))
	for _, info := range r.plugins {
		infos = append(infos, info)
	}
	return infos
}

// Count returns the number of registered plugins
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.plugins)
}

// Close closes all plugins
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []string
	for name, info := range r.plugins {
		if err := info.Plugin.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", name, err))
		}
	}

	r.plugins = make(map[string]*PluginInfo)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing plugins: %s", strings.Join(errs, "; "))
	}

	return nil
}

// ============ Hook Execution ============

// ExecuteBeforeCreate executes BeforeCreate hooks on all HookPlugins
func (r *Registry) ExecuteBeforeCreate(ctx context.Context, model interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.BeforeCreate(ctx, model)
	})
}

// ExecuteAfterCreate executes AfterCreate hooks on all HookPlugins
func (r *Registry) ExecuteAfterCreate(ctx context.Context, model interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.AfterCreate(ctx, model)
	})
}

// ExecuteBeforeUpdate executes BeforeUpdate hooks on all HookPlugins
func (r *Registry) ExecuteBeforeUpdate(ctx context.Context, model interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.BeforeUpdate(ctx, model)
	})
}

// ExecuteAfterUpdate executes AfterUpdate hooks on all HookPlugins
func (r *Registry) ExecuteAfterUpdate(ctx context.Context, model interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.AfterUpdate(ctx, model)
	})
}

// ExecuteBeforeDelete executes BeforeDelete hooks on all HookPlugins
func (r *Registry) ExecuteBeforeDelete(ctx context.Context, model interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.BeforeDelete(ctx, model)
	})
}

// ExecuteAfterDelete executes AfterDelete hooks on all HookPlugins
func (r *Registry) ExecuteAfterDelete(ctx context.Context, model interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.AfterDelete(ctx, model)
	})
}

// ExecuteBeforeQuery executes BeforeQuery hooks on all HookPlugins
func (r *Registry) ExecuteBeforeQuery(ctx context.Context, query string, args ...interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.BeforeQuery(ctx, query, args...)
	})
}

// ExecuteAfterQuery executes AfterQuery hooks on all HookPlugins
func (r *Registry) ExecuteAfterQuery(ctx context.Context, result interface{}) error {
	return r.executeHook(func(p HookPlugin) error {
		return p.AfterQuery(ctx, result)
	})
}

// executeHook executes a hook function on all HookPlugins
func (r *Registry) executeHook(hook func(HookPlugin) error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, info := range r.plugins {
		if hp, ok := info.Plugin.(HookPlugin); ok {
			if err := hook(hp); err != nil {
				return WrapError(info.Name, err)
			}
		}
	}
	return nil
}

// ============ Query Interception ============

// InterceptQuery executes InterceptQuery on all QueryPlugins
func (r *Registry) InterceptQuery(ctx context.Context, query string, args ...interface{}) (string, []interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	interceptedQuery := query
	interceptedArgs := args

	for _, info := range r.plugins {
		if qp, ok := info.Plugin.(QueryPlugin); ok {
			newQuery, newArgs, err := qp.InterceptQuery(ctx, interceptedQuery, interceptedArgs...)
			if err != nil {
				return "", nil, WrapError(info.Name, err)
			}
			interceptedQuery = newQuery
			interceptedArgs = newArgs
		}
	}

	return interceptedQuery, interceptedArgs, nil
}

// InterceptResult executes InterceptResult on all QueryPlugins
func (r *Registry) InterceptResult(ctx context.Context, result interface{}) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, info := range r.plugins {
		if qp, ok := info.Plugin.(QueryPlugin); ok {
			if err := qp.InterceptResult(ctx, result); err != nil {
				return WrapError(info.Name, err)
			}
		}
	}
	return nil
}

// ============ Transaction Hooks ============

// ExecuteBeforeBegin executes BeforeBegin hooks on all TransactionPlugins
func (r *Registry) ExecuteBeforeBegin(ctx context.Context) error {
	return r.executeTransactionHook(func(p TransactionPlugin) error {
		return p.BeforeBegin(ctx)
	})
}

// ExecuteAfterBegin executes AfterBegin hooks on all TransactionPlugins
func (r *Registry) ExecuteAfterBegin(ctx context.Context, txID string) error {
	return r.executeTransactionHook(func(p TransactionPlugin) error {
		return p.AfterBegin(ctx, txID)
	})
}

// ExecuteBeforeCommit executes BeforeCommit hooks on all TransactionPlugins
func (r *Registry) ExecuteBeforeCommit(ctx context.Context, txID string) error {
	return r.executeTransactionHook(func(p TransactionPlugin) error {
		return p.BeforeCommit(ctx, txID)
	})
}

// ExecuteAfterCommit executes AfterCommit hooks on all TransactionPlugins
func (r *Registry) ExecuteAfterCommit(ctx context.Context, txID string) error {
	return r.executeTransactionHook(func(p TransactionPlugin) error {
		return p.AfterCommit(ctx, txID)
	})
}

// ExecuteBeforeRollback executes BeforeRollback hooks on all TransactionPlugins
func (r *Registry) ExecuteBeforeRollback(ctx context.Context, txID string) error {
	return r.executeTransactionHook(func(p TransactionPlugin) error {
		return p.BeforeRollback(ctx, txID)
	})
}

// ExecuteAfterRollback executes AfterRollback hooks on all TransactionPlugins
func (r *Registry) ExecuteAfterRollback(ctx context.Context, txID string) error {
	return r.executeTransactionHook(func(p TransactionPlugin) error {
		return p.AfterRollback(ctx, txID)
	})
}

// executeTransactionHook executes a transaction hook function on all TransactionPlugins
func (r *Registry) executeTransactionHook(hook func(TransactionPlugin) error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, info := range r.plugins {
		if tp, ok := info.Plugin.(TransactionPlugin); ok {
			if err := hook(tp); err != nil {
				return WrapError(info.Name, err)
			}
		}
	}
	return nil
}

// ============ Validation Hooks ============

// ExecuteValidateModel executes ValidateModel on all ValidationPlugins
func (r *Registry) ExecuteValidateModel(ctx context.Context, model interface{}) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, info := range r.plugins {
		if vp, ok := info.Plugin.(ValidationPlugin); ok {
			if err := vp.ValidateModel(ctx, model); err != nil {
				return WrapError(info.Name, err)
			}
		}
	}
	return nil
}

// ExecuteValidateField executes ValidateField on all ValidationPlugins
func (r *Registry) ExecuteValidateField(ctx context.Context, model interface{}, field string, value interface{}) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, info := range r.plugins {
		if vp, ok := info.Plugin.(ValidationPlugin); ok {
			if err := vp.ValidateField(ctx, model, field, value); err != nil {
				return WrapError(info.Name, err)
			}
		}
	}
	return nil
}
