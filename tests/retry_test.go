package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomodul/db"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// mockDriver is a mock driver for testing retry
type mockDriver struct {
	attemptCount int
	failUntil    int
	errorToReturn error
	shouldFail   bool
}

func (m *mockDriver) Name() string {
	return "mock"
}

func (m *mockDriver) Type() dialect.DriverType {
	return dialect.TypeSQL
}

func (m *mockDriver) Initialize(cfg *dialect.Config) error {
	return nil
}

func (m *mockDriver) Close() error {
	return nil
}

func (m *mockDriver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	m.attemptCount++

	if m.failUntil > 0 && m.attemptCount <= m.failUntil {
		return nil, errors.New("connection timeout")
	}

	if m.shouldFail {
		return nil, m.errorToReturn
	}

	return &dialect.Result{
		Data:         []interface{}{map[string]interface{}{"id": 1, "name": "test"}},
		Count:        1,
		RowsAffected: 1,
	}, nil
}

func (m *mockDriver) Capabilities() *dialect.Capabilities {
	return &dialect.Capabilities{
		Query: dialect.QueryCapabilities{
			Create: true,
			Read:   true,
			Update: true,
			Delete: true,
		},
		Transaction: dialect.TransactionCapabilities{
			Supported: true,
		},
	}
}

func (m *mockDriver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	return nil, dialect.ErrNotSupported
}

func (m *mockDriver) Migrator() dialect.Migrator {
	return &dialect.NoOpMigrator{}
}

func (m *mockDriver) Health() (*dialect.HealthStatus, error) {
	return dialect.NewHealthyStatus(0), nil
}

func (m *mockDriver) Ping(ctx context.Context) error {
	return nil
}

// TestRetryableDriver_SuccessAfterRetries tests that retry works on transient errors
func TestRetryableDriver_SuccessAfterRetries(t *testing.T) {
	mock := &mockDriver{
		failUntil: 2, // Fail first 2 attempts
	}

	retryableDriver := dialect.NewRetryableDriver(mock, 3, 10*time.Millisecond, 100*time.Millisecond)

	ctx := context.Background()
	q := &query.Query{
		Operation:  query.OpFind,
		Collection: "test",
	}

	result, err := retryableDriver.Execute(ctx, q)

	if err != nil {
		t.Fatalf("Expected success after retries, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	if mock.attemptCount != 3 {
		t.Errorf("Expected 3 attempts, got %d", mock.attemptCount)
	}
}

// TestRetryableDriver_NonTransientError tests that non-transient errors are not retried
func TestRetryableDriver_NonTransientError(t *testing.T) {
	mock := &mockDriver{
		shouldFail:   true,
		errorToReturn: dialect.ErrNotFound,
	}

	retryableDriver := dialect.NewRetryableDriver(mock, 3, 10*time.Millisecond, 100*time.Millisecond)

	ctx := context.Background()
	q := &query.Query{
		Operation:  query.OpFind,
		Collection: "test",
	}

	_, err := retryableDriver.Execute(ctx, q)

	if err == nil {
		t.Fatal("Expected error for non-transient error, got nil")
	}

	if !errors.Is(err, dialect.ErrNotFound) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// Should only attempt once for non-transient errors
	if mock.attemptCount != 1 {
		t.Errorf("Expected 1 attempt for non-transient error, got %d", mock.attemptCount)
	}
}

// TestRetryableDriver_DuplicateKeyError tests that duplicate key errors are not retried
func TestRetryableDriver_DuplicateKeyError(t *testing.T) {
	mock := &mockDriver{
		shouldFail:    true,
		errorToReturn: dialect.ErrDuplicate,
	}

	retryableDriver := dialect.NewRetryableDriver(mock, 3, 10*time.Millisecond, 100*time.Millisecond)

	ctx := context.Background()
	q := &query.Query{
		Operation:  query.OpCreate,
		Collection: "test",
	}

	_, err := retryableDriver.Execute(ctx, q)

	if err == nil {
		t.Fatal("Expected error for duplicate key, got nil")
	}

	// Should only attempt once for duplicate key errors
	if mock.attemptCount != 1 {
		t.Errorf("Expected 1 attempt for duplicate key error, got %d", mock.attemptCount)
	}
}

// TestRetryableDriver_MaxRetriesExceeded tests that it returns error after max retries
func TestRetryableDriver_MaxRetriesExceeded(t *testing.T) {
	mock := &mockDriver{
		shouldFail: true,
		errorToReturn: errors.New("connection timeout"),
	}

	retryableDriver := dialect.NewRetryableDriver(mock, 2, 10*time.Millisecond, 50*time.Millisecond)

	ctx := context.Background()
	q := &query.Query{
		Operation:  query.OpFind,
		Collection: "test",
	}

	_, err := retryableDriver.Execute(ctx, q)

	if err == nil {
		t.Fatal("Expected error after max retries, got nil")
	}

	// Should attempt maxRetries + 1 times (initial + retries)
	expectedAttempts := 3 // 1 initial + 2 retries
	if mock.attemptCount != expectedAttempts {
		t.Errorf("Expected %d attempts, got %d", expectedAttempts, mock.attemptCount)
	}
}

// TestRetryableDriver_ContextCancellation tests that retry respects context cancellation
func TestRetryableDriver_ContextCancellation(t *testing.T) {
	mock := &mockDriver{
		shouldFail: true,
		errorToReturn: errors.New("connection timeout"),
	}

	retryableDriver := dialect.NewRetryableDriver(mock, 5, 10*time.Millisecond, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	q := &query.Query{
		Operation:  query.OpFind,
		Collection: "test",
	}

	_, err := retryableDriver.Execute(ctx, q)

	if err == nil {
		t.Fatal("Expected context error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

// TestRetryableDriver_NoRetryOnSuccess tests that no retry happens on success
func TestRetryableDriver_NoRetryOnSuccess(t *testing.T) {
	mock := &mockDriver{
		failUntil: 0, // No failures
	}

	retryableDriver := dialect.NewRetryableDriver(mock, 3, 10*time.Millisecond, 100*time.Millisecond)

	ctx := context.Background()
	q := &query.Query{
		Operation:  query.OpFind,
		Collection: "test",
	}

	result, err := retryableDriver.Execute(ctx, q)

	if err != nil {
		t.Fatalf("Expected success on first attempt, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	if mock.attemptCount != 1 {
		t.Errorf("Expected 1 attempt for immediate success, got %d", mock.attemptCount)
	}
}

// TestIsTransientError tests various transient error patterns
func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		isTransient bool
	}{
		{"Connection Refused", errors.New("connection refused"), true},
		{"Connection Reset", errors.New("connection reset"), true},
		{"Broken Pipe", errors.New("broken pipe"), true},
		{"Timeout", errors.New("timeout"), true},
		{"Deadline Exceeded", errors.New("deadline exceeded"), true},
		{"Temporary Failure", errors.New("temporary failure"), true},
		{"Not Found (non-transient)", dialect.ErrNotFound, false},
		{"Duplicate (non-transient)", dialect.ErrDuplicate, false},
		{"Transaction Done (non-transient)", dialect.ErrTxDone, false},
		{"Generic Error", errors.New("some other error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't directly test isTransientError as it's not exported
			// But we can test it through Execute
			mock := &mockDriver{
				shouldFail:    true,
				errorToReturn: tt.err,
			}

			retryableDriver := dialect.NewRetryableDriver(mock, 2, 5*time.Millisecond, 20*time.Millisecond)

			ctx := context.Background()
			q := &query.Query{
				Operation:  query.OpFind,
				Collection: "test",
			}

			retryableDriver.Execute(ctx, q)

			if tt.isTransient && mock.attemptCount != 3 { // 1 initial + 2 retries
				t.Errorf("Expected multiple retries for transient error '%s', got %d attempts", tt.name, mock.attemptCount)
			} else if !tt.isTransient && mock.attemptCount != 1 {
				t.Errorf("Expected no retry for non-transient error '%s', got %d attempts", tt.name, mock.attemptCount)
			}
		})
	}
}

// TestRetryWithDB tests retry through the DB interface
func TestRetryWithDB(t *testing.T) {
	mock := &mockDriver{
		failUntil: 1, // Fail first attempt
	}

	cfg := db.Config{
		DSN:            "mock://test",
		RetryMaxRetries: 2,
		RetryBaseDelay:  10 * time.Millisecond,
		RetryMaxDelay:   100 * time.Millisecond,
	}

	database, err := db.OpenWithDriver(mock, cfg)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	q := &query.Query{
		Operation:  query.OpFind,
		Collection: "test",
	}

	result, err := database.Execute(ctx, q)
	if err != nil {
		t.Fatalf("Expected success after retry, got error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected result, got nil")
	}
}
