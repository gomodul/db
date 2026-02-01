package db

import "time"

// Config holds the configuration for opening a database connection.
type Config struct {
	// DSN is the data source name / connection string.
	//
	// Examples:
	//   PostgreSQL: "postgres://user:pass@localhost:5432/dbname?sslmode=disable"
	//   MySQL:      "user:pass@tcp(localhost:3306)/dbname?parseTime=true"
	//   SQLite:     "file:test.db?cache=shared"
	//   MongoDB:    "mongodb://localhost:27017/dbname"
	//   Redis:      "redis://localhost:6379/0"
	DSN string

	// MaxOpenConns sets the maximum number of open connections to the database.
	// 0 means unlimited. Only applies to SQL databases.
	MaxOpenConns int

	// MaxIdleConns sets the maximum number of idle connections in the pool.
	// Only applies to SQL databases.
	MaxIdleConns int

	// ConnMaxLifetime sets the maximum amount of time a connection may be reused.
	// Only applies to SQL databases.
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime sets the maximum amount of time a connection may be idle.
	// Only applies to SQL databases.
	ConnMaxIdleTime time.Duration

	// Retry configuration for transient failures.
	RetryMaxRetries int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
}
