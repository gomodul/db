// Package retry provides retry mechanisms for database operations.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/gomodul/db"
)

// Config holds retry configuration.
type Config struct {
	// MaxRetries is the maximum number of retry attempts.
	MaxRetries int

	// BaseDelay is the initial delay before the first retry.
	BaseDelay time.Duration

	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration

	// Multiplier is the factor by which the delay increases after each retry.
	Multiplier float64

	// Jitter enables random jitter to prevent thundering herd.
	Jitter bool
}

// DefaultConfig returns a default retry configuration.
func DefaultConfig() Config {
	return Config{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   1 * time.Second,
		Multiplier: 2.0,
		Jitter:     true,
	}
}

// IsTransient returns true if the error is a transient failure that should be retried.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}

	// Don't retry these specific errors
	if errors.Is(err, db.ErrNotFound) || errors.Is(err, db.ErrDuplicate) {
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
	}

	for _, pattern := range transientPatterns {
		if contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// Do executes the given function with retry logic.
func Do(ctx context.Context, cfg Config, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retrying
			delay := calculateDelay(cfg, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		// Check context before attempting
		if err := ctx.Err(); err != nil {
			return err
		}

		// Execute the function
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry if this is not a transient error
		if !IsTransient(err) {
			return err
		}
	}

	return fmt.Errorf("retry failed after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}

// DoWithResult executes the given function with retry logic and returns a result.
func DoWithResult[T any](ctx context.Context, cfg Config, fn func() (T, error)) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := calculateDelay(cfg, attempt)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(delay):
			}
		}

		if err := ctx.Err(); err != nil {
			return zero, err
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		if !IsTransient(err) {
			return zero, err
		}
	}

	return zero, fmt.Errorf("retry failed after %d attempts: %w", cfg.MaxRetries+1, lastErr)
}

// calculateDelay calculates the delay for the given attempt with exponential backoff.
func calculateDelay(cfg Config, attempt int) time.Duration {
	// Calculate exponential backoff
	delay := time.Duration(float64(cfg.BaseDelay) * pow(cfg.Multiplier, float64(attempt-1)))

	// Cap at max delay
	if delay > cfg.MaxDelay {
		delay = cfg.MaxDelay
	}

	// Add jitter if enabled
	if cfg.Jitter {
		// Add random jitter up to ±25% of the delay
		jitter := time.Duration(rand.Int63n(int64(delay) / 4))
		delay = delay - jitter/2 + jitter
	}

	return delay
}

// pow calculates base^exp for floats.
func pow(base, exp float64) float64 {
	if exp == 0 {
		return 1
	}
	result := base
	for i := 1; i < int(exp); i++ {
		result *= base
	}
	return result
}

// contains checks if a string contains a substring (case-insensitive).
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
		 len(s) > len(substr) && (
			s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
		 indexOf(s, substr) >= 0))
}

// indexOf finds the index of a substring in a string.
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
