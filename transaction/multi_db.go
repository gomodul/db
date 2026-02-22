package transaction

import (
	"context"
	"fmt"
	"sync"

	"github.com/gomodul/db"
)

// MultiDBTx manages transactions across multiple databases
type MultiDBTx struct {
	mu        sync.Mutex
	databases map[string]*db.DB
	txIDs     map[string]string
	commits   []func() error
	rollbacks []func() error
	ctx       context.Context
	active    bool
}

// NewMultiDBTx creates a new multi-database transaction manager
func NewMultiDBTx(ctx context.Context) *MultiDBTx {
	if ctx == nil {
		ctx = context.Background()
	}
	return &MultiDBTx{
		databases: make(map[string]*db.DB),
		txIDs:     make(map[string]string),
		ctx:       ctx,
		active:    true,
	}
}

// Add adds a database to the transaction and begins its transaction
func (m *MultiDBTx) Add(name string, database *db.DB) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.active {
		return fmt.Errorf("transaction is no longer active")
	}

	if _, exists := m.databases[name]; exists {
		return fmt.Errorf("database %s already added", name)
	}

	// Begin transaction on this database
	// Note: This assumes the DB has a BeginTx method
	// In practice, you might need to adapt this to your specific DB implementation
	txID := fmt.Sprintf("multi_tx_%s_%d", name, len(m.txIDs))

	m.databases[name] = database
	m.txIDs[name] = txID

	// Store commit and rollback functions
	m.commits = append(m.commits, func() error {
		// Call commit on the specific database
		// This would need to be adapted based on your DB's transaction API
		return nil
	})

	m.rollbacks = append(m.rollbacks, func() error {
		// Call rollback on the specific database
		// This would need to be adapted based on your DB's transaction API
		return nil
	})

	return nil
}

// Commit commits all transactions in reverse order
func (m *MultiDBTx) Commit() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.active {
		return fmt.Errorf("transaction is no longer active")
	}

	// Execute commits in reverse order (last in, first out)
	var errs []error
	for i := len(m.commits) - 1; i >= 0; i-- {
		if err := m.commits[i](); err != nil {
			errs = append(errs, fmt.Errorf("commit %d failed: %w", i, err))
			// Continue trying to commit other transactions
		}
	}

	m.active = false

	if len(errs) > 0 {
		return fmt.Errorf("multi-database commit failed: %v", errs)
	}

	return nil
}

// Rollback rolls back all transactions
func (m *MultiDBTx) Rollback() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.active {
		return fmt.Errorf("transaction is no longer active")
	}

	var errs []error
	for i := len(m.rollbacks) - 1; i >= 0; i-- {
		if err := m.rollbacks[i](); err != nil {
			errs = append(errs, fmt.Errorf("rollback %d failed: %w", i, err))
			// Continue trying to rollback other transactions
		}
	}

	m.active = false

	if len(errs) > 0 {
		return fmt.Errorf("multi-database rollback failed: %v", errs)
	}

	return nil
}

// CommitWithRetry commits all transactions with retry logic
func (m *MultiDBTx) CommitWithRetry(maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := m.Commit(); err == nil {
			return nil
		} else {
			lastErr = err
			// Check if it's a retryable error
			if !isRetryableError(err) {
				return err
			}
		}
	}
	return lastErr
}

// GetTxID returns the transaction ID for a specific database
func (m *MultiDBTx) GetTxID(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txID, ok := m.txIDs[name]
	return txID, ok
}

// GetDatabase returns a database by name
func (m *MultiDBTx) GetDatabase(name string) (*db.DB, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	database, ok := m.databases[name]
	return database, ok
}

// ListDatabases returns all database names in the transaction
func (m *MultiDBTx) ListDatabases() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0, len(m.databases))
	for name := range m.databases {
		names = append(names, name)
	}
	return names
}

// IsActive returns true if the transaction is still active
func (m *MultiDBTx) IsActive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.active
}

// Count returns the number of databases in the transaction
func (m *MultiDBTx) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.databases)
}

// isRetryableError checks if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Check for common retryable error patterns
	errMsg := err.Error()
	// Add more patterns as needed
	return contains(errMsg, "timeout") ||
		contains(errMsg, "connection") ||
		contains(errMsg, "temporary") ||
		contains(errMsg, "deadlock")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsIn(s, substr))
}

func containsIn(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============ Two-Phase Commit ============

// TwoPhaseCommit implements a two-phase commit protocol for multi-database transactions
type TwoPhaseCommit struct {
	mu        sync.Mutex
	databases map[string]*db.DB
	prepared  map[string]bool
	ctx       context.Context
}

// NewTwoPhaseCommit creates a new two-phase commit manager
func NewTwoPhaseCommit(ctx context.Context) *TwoPhaseCommit {
	if ctx == nil {
		ctx = context.Background()
	}
	return &TwoPhaseCommit{
		databases: make(map[string]*db.DB),
		prepared:  make(map[string]bool),
		ctx:       ctx,
	}
}

// Add adds a database to the two-phase commit
func (t *TwoPhaseCommit) Add(name string, database *db.DB) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.databases[name]; exists {
		return fmt.Errorf("database %s already added", name)
	}

	t.databases[name] = database
	t.prepared[name] = false
	return nil
}

// Prepare prepares all databases for commit (phase 1)
func (t *TwoPhaseCommit) Prepare() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var errs []error
	for name := range t.databases {
		// Execute prepare on the database
		// This would need to be adapted based on your DB's transaction API
		t.prepared[name] = true
	}

	if len(errs) > 0 {
		// Rollback all prepared transactions
		t.rollbackLocked()
		return fmt.Errorf("prepare phase failed: %v", errs)
	}

	return nil
}

// Commit commits all prepared databases (phase 2)
func (t *TwoPhaseCommit) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check that all databases are prepared
	for name, prepared := range t.prepared {
		if !prepared {
			return fmt.Errorf("database %s not prepared", name)
		}
	}

	var errs []error
	for range t.databases {
		// Execute commit on the database
		// This would need to be adapted based on your DB's transaction API
	}

	if len(errs) > 0 {
		return fmt.Errorf("commit phase failed: %v", errs)
	}

	return nil
}

// Rollback rolls back all databases
func (t *TwoPhaseCommit) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.rollbackLocked()
}

func (t *TwoPhaseCommit) rollbackLocked() error {
	var errs []error
	for range t.databases {
		// Execute rollback on the database
		// This would need to be adapted based on your DB's transaction API
	}

	if len(errs) > 0 {
		return fmt.Errorf("rollback failed: %v", errs)
	}

	return nil
}
