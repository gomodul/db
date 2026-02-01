package dialect

import (
	"context"
	"fmt"
	"time"

	"github.com/gomodul/db/query"
)

// RetryableDriver wraps a Driver with retry capability
type RetryableDriver struct {
	driver   Driver
	maxRetry int
	baseDelay time.Duration
	maxDelay time.Duration
}

// NewRetryableDriver creates a new retryable driver wrapper
func NewRetryableDriver(driver Driver, maxRetry int, baseDelay, maxDelay time.Duration) *RetryableDriver {
	return &RetryableDriver{
		driver:    driver,
		maxRetry:  maxRetry,
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

// Name returns the driver name
func (r *RetryableDriver) Name() string {
	return r.driver.Name()
}

// Type returns the driver type
func (r *RetryableDriver) Type() DriverType {
	return r.driver.Type()
}

// Initialize initializes the driver
func (r *RetryableDriver) Initialize(cfg *Config) error {
	return r.driver.Initialize(cfg)
}

// Close closes the driver
func (r *RetryableDriver) Close() error {
	return r.driver.Close()
}

// Capabilities returns the driver's capabilities
func (r *RetryableDriver) Capabilities() *Capabilities {
	return r.driver.Capabilities()
}

// Execute executes a query with retry on transient failures
func (r *RetryableDriver) Execute(ctx context.Context, q *query.Query) (*Result, error) {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetry; attempt++ {
		if attempt > 0 {
			// Wait before retrying
			delay := r.calculateDelay(attempt)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		// Check context before attempting
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Execute the query
		result, err := r.driver.Execute(ctx, q)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Don't retry if this is not a transient error
		if !isTransientError(err) {
			return nil, err
		}
	}

	return nil, fmt.Errorf("retry failed after %d attempts: %w", r.maxRetry+1, lastErr)
}

// BeginTx begins a transaction (no retry for transactions)
func (r *RetryableDriver) BeginTx(ctx context.Context) (Transaction, error) {
	if t, ok := r.driver.(Transactor); ok {
		return t.BeginTx(ctx)
	}
	return nil, ErrNotSupported
}

// Migrator returns the migrator
func (r *RetryableDriver) Migrator() Migrator {
	if m, ok := r.driver.(SchemaMigrator); ok {
		return m.Migrator()
	}
	return &NoOpMigrator{}
}

// Health checks the health status
func (r *RetryableDriver) Health() (*HealthStatus, error) {
	if h, ok := r.driver.(HealthChecker); ok {
		return h.Health()
	}
	return NewUnhealthyStatus("health check not supported"), nil
}

// Ping pings the database
func (r *RetryableDriver) Ping(ctx context.Context) error {
	if c, ok := r.driver.(Conn); ok {
		return c.Ping(ctx)
	}
	return ErrNotSupported
}

// calculateDelay calculates exponential backoff delay
func (r *RetryableDriver) calculateDelay(attempt int) time.Duration {
	// Calculate exponential backoff: baseDelay * 2^(attempt-1)
	delay := r.baseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
	}

	// Cap at max delay
	if delay > r.maxDelay {
		delay = r.maxDelay
	}

	return delay
}

// isTransientError returns true if the error is a transient failure
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Don't retry these specific errors
	if err == ErrNotFound || err == ErrDuplicate || err == ErrTxDone {
		return false
	}

	// Check for transient error patterns
	errStr := err.Error()

	// Network errors
	transientPatterns := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"timeout",
		"deadline exceeded",
		"temporary failure",
		"transport is closing",
		"i/o timeout",
		"no such host",
		"cannot assign requested address",
		"connection timed out",
		"TLS handshake timeout",
		"network is unreachable",
		"host is down",
		"connection lost",
		"temporary",
	}

	for _, pattern := range transientPatterns {
		if contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) && indexOf(s, substr) >= 0)
}

// indexOf finds the index of a substring in a string
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
