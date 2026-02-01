package dialect

import (
	"context"

	"github.com/gomodul/db/query"
)

// Dialect is the core interface for all database backends.
// Following Interface Segregation Principle - only essential methods.
type Dialect interface {
	// Core identity
	Name() string
	Type() DriverType

	// Connection lifecycle
	Initialize(cfg *Config) error
	Close() error

	// Query execution (universal)
	Execute(ctx context.Context, q *query.Query) (*Result, error)

	// Capability discovery
	Capabilities() *Capabilities
}

// Conn represents a database connection
type Conn interface {
	Dialect
	Ping(ctx context.Context) error
}

// Optional interfaces - detect with type assertion

// Transactor is an optional interface for databases that support transactions
type Transactor interface {
	BeginTx(ctx context.Context) (Transaction, error)
}

// Migrator is an optional interface for databases that support schema migration
type SchemaMigrator interface {
	Migrator() Migrator
}

// HealthChecker is an optional interface for databases that support health checks
type HealthChecker interface {
	Health() (*HealthStatus, error)
}

// DialectWithFeatures combines Dialect with feature detection
type DialectWithFeatures interface {
	Dialect
	Features() Feature
}

// Composite interfaces for convenience

// FullDialect includes all optional interfaces
type FullDialect interface {
	Dialect
	Transactor
	SchemaMigrator
	HealthChecker
}

// Helper functions to check optional interfaces

// CanTransact returns true if the dialect supports transactions
func CanTransact(d Dialect) bool {
	_, ok := d.(Transactor)
	return ok
}

// CanMigrate returns true if the dialect supports schema migration
func CanMigrate(d Dialect) bool {
	_, ok := d.(SchemaMigrator)
	return ok
}

// CanHealthCheck returns true if the dialect supports health checks
func CanHealthCheck(d Dialect) bool {
	_, ok := d.(HealthChecker)
	return ok
}

// HasFeature checks if the dialect has a specific feature
func HasFeature(d Dialect, feature Feature) bool {
	if df, ok := d.(DialectWithFeatures); ok {
		return df.Features().Has(feature)
	}
	// Fallback to checking capabilities
	caps := d.Capabilities()
	return checkCapabilitiesForFeature(caps, feature)
}

// checkCapabilitiesForFeature checks capabilities for a given feature
func checkCapabilitiesForFeature(caps *Capabilities, feature Feature) bool {
	switch feature {
	case FeatureCreate:
		return caps.Query.Create
	case FeatureRead:
		return caps.Query.Read
	case FeatureUpdate:
		return caps.Query.Update
	case FeatureDelete:
		return caps.Query.Delete
	case FeatureBatchCreate:
		return caps.Query.BatchCreate
	case FeatureBatchUpdate:
		return caps.Query.BatchUpdate
	case FeatureBatchDelete:
		return caps.Query.BatchDelete
	case FeatureJoins:
		return caps.Query.Joins
	case FeatureTransactions:
		return caps.Transaction.Supported
	case FeatureAutoMigrate:
		return caps.Schema.AutoMigrate
	case FeatureSavepoints:
		return caps.Transaction.Savepoints
	default:
		return false
	}
}

// BeginTx is a convenience function that safely starts a transaction
// Returns nil if the dialect doesn't support transactions
func BeginTx(ctx context.Context, d Dialect) (Transaction, error) {
	if t, ok := d.(Transactor); ok {
		return t.BeginTx(ctx)
	}
	return nil, ErrNotSupported
}

// Health is a convenience function that safely gets health status
func Health(d Dialect) (*HealthStatus, error) {
	if h, ok := d.(HealthChecker); ok {
		return h.Health()
	}
	return NewUnhealthyStatus("health check not supported"), nil
}

// GetMigrator is a convenience function that safely gets migrator
func GetMigrator(d Dialect) Migrator {
	if m, ok := d.(SchemaMigrator); ok {
		return m.Migrator()
	}
	return &NoOpMigrator{}
}

// DriverFactory is a function that creates a new driver instance
type DriverFactory func() Driver

// driverRegistry holds all registered drivers
var driverRegistry = make(map[string]DriverFactory)

// Register registers a driver factory for a given name
func Register(name string, factory DriverFactory) {
	driverRegistry[name] = factory
}

// GetDriver returns a driver instance by name, or nil if not found
func GetDriver(name string) Driver {
	if factory, ok := driverRegistry[name]; ok {
		return factory()
	}
	return nil
}

// RegisteredDrivers returns all registered driver names
func RegisteredDrivers() []string {
	names := make([]string, 0, len(driverRegistry))
	for name := range driverRegistry {
		names = append(names, name)
	}
	return names
}
