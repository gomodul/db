package dialect

import "time"

// Driver is an alias for Dialect for backward compatibility
// Deprecated: Use Dialect instead
type Driver = Dialect

// DriverType represents the category of driver
type DriverType string

const (
	TypeSQL         DriverType = "sql"
	TypeNoSQL       DriverType = "nosql"
	TypeAPI         DriverType = "api"
	TypeRPC         DriverType = "rpc"
	TypeGraphQL     DriverType = "graphql"
	TypeMessageQueue DriverType = "mq"
	TypeKVStore     DriverType = "kv"
	TypeSearch      DriverType = "search"
)

// Config holds driver configuration
type Config struct {
	// Connection
	DSN string

	// Database name (for databases that require it)
	Database string

	// Driver-specific options
	Options map[string]interface{}

	// Pool settings (for SQL drivers)
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// Retry configuration
	RetryMaxRetries int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration

	// Timeout
	QueryTimeout   time.Duration
	ConnectTimeout time.Duration
}

// Result is the universal result structure
type Result struct {
	Data         []interface{}
	Count        int64
	RowsAffected int64
	LastInsertID interface{}
	Cursor       interface{}
	Error        error
	Metadata     map[string]interface{}
}

// IsEmpty returns true if no data was returned
func (r *Result) IsEmpty() bool {
	return len(r.Data) == 0
}

// One returns the first result or nil
func (r *Result) One() interface{} {
	if len(r.Data) > 0 {
		return r.Data[0]
	}
	return nil
}
