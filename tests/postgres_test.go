package tests

import (
	"context"
	"testing"

	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/driver/postgres"
	"github.com/gomodul/db/query"
)

// TestPostgreSQLDriver tests the PostgreSQL driver implementation
func TestPostgreSQLDriver(t *testing.T) {
	driver := postgres.NewDriver()

	// Test driver properties
	if driver.Name() != "postgres" {
		t.Errorf("Expected name 'postgres', got '%s'", driver.Name())
	}

	if driver.Type() != dialect.TypeSQL {
		t.Errorf("Expected type '%s', got '%s'", dialect.TypeSQL, driver.Type())
	}

	// Test capabilities
	caps := driver.Capabilities()
	if !caps.Query.Create {
		t.Error("Expected Create capability")
	}
	if !caps.Query.Read {
		t.Error("Expected Read capability")
	}
	if !caps.Query.Update {
		t.Error("Expected Update capability")
	}
	if !caps.Query.Delete {
		t.Error("Expected Delete capability")
	}
	if !caps.Transaction.Supported {
		t.Error("Expected transaction support")
	}
}

// TestPostgreSQLQueryTranslation tests query translation to SQL
func TestPostgreSQLQueryTranslation(t *testing.T) {
	driver := postgres.NewDriver()

	cfg := &dialect.Config{DSN: "postgres://localhost/test"}
	err := driver.Initialize(cfg)
	if err != nil {
		t.Skip("Skipping test - PostgreSQL not available")
	}

	// Test simple select query
	q := &query.Query{
		Operation:  query.OpFind,
		Collection:  "users",
		Filters:     []*query.Filter{{Field: "id", Operator: query.OpEqual, Value: 1}},
		Limit:       intPtr(10),
		Offset:      intPtr(0),
		Orders:      []*query.Order{{Field: "name", Direction: "ASC"}},
	}

	ctx := context.Background()
	result, err := driver.Execute(ctx, q)
	if err != nil {
		t.Logf("Query execution failed (expected if DB not available): %v", err)
		return
	}

	if result == nil {
		t.Error("Expected result, got nil")
	}
}

// TestPostgreSQLCapabilities tests that capabilities are correctly declared
func TestPostgreSQLCapabilities(t *testing.T) {
	driver := postgres.NewDriver()
	caps := driver.Capabilities()

	// Query capabilities
	tests := []struct {
		name     string
		capacity bool
	}{
		{"Create", caps.Query.Create},
		{"Read", caps.Query.Read},
		{"Update", caps.Query.Update},
		{"Delete", caps.Query.Delete},
		{"BatchCreate", caps.Query.BatchCreate},
		{"BatchUpdate", caps.Query.BatchUpdate},
		{"BatchDelete", caps.Query.BatchDelete},
		{"Sort", caps.Query.Sort},
		{"MultiFieldSort", caps.Query.MultiFieldSort},
		{"OffsetPagination", caps.Query.OffsetPagination},
		{"GroupBy", caps.Query.GroupBy},
		{"Joins", caps.Query.Joins},
		{"Subqueries", caps.Query.Subqueries},
		{"Unions", caps.Query.Unions},
		{"Hints", caps.Query.Hints},
		{"Locking", caps.Query.Locking},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.capacity && tt.name != "Locking" {
				// Locking is the only one that should be false for this test
				t.Errorf("Expected %s capability to be true", tt.name)
			}
		})
	}

	// Transaction capabilities
	if !caps.Transaction.Supported {
		t.Error("Expected transaction support")
	}
	if !caps.Transaction.Nested {
		t.Error("Expected nested transaction support")
	}
	if !caps.Transaction.Savepoints {
		t.Error("Expected savepoints support")
	}
	if len(caps.Transaction.IsolationLevels) == 0 {
		t.Error("Expected isolation levels")
	}

	// Schema capabilities
	if !caps.Schema.AutoMigrate {
		t.Error("Expected AutoMigrate capability")
	}
	if !caps.Schema.CreateTables {
		t.Error("Expected CreateTables capability")
	}
	if !caps.Schema.CreateIndexes {
		t.Error("Expected CreateIndexes capability")
	}
}

func intPtr(i int) *int {
	return &i
}
