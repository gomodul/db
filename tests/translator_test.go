package tests

import (
	"testing"

	"github.com/gomodul/db/query"
	"github.com/gomodul/db/translator"
	"github.com/gomodul/db/driver/postgres"
)

// TestSQLTranslator tests the SQL translator
func TestSQLTranslator(t *testing.T) {
	dialect := &postgres.PostgresDialect{}
	trans := translator.NewSQLTranslator(dialect)

	// Test simple SELECT query
	t.Run("SimpleSelect", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpFind,
			Collection: "users",
			Filters: []*query.Filter{
				{Field: "id", Operator: query.OpEqual, Value: 1},
			},
			Limit:  intPtr(10),
			Offset: intPtr(0),
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		if sqlQuery.SQL == "" {
			t.Error("Expected SQL query, got empty string")
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test INSERT query
	t.Run("InsertQuery", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpCreate,
			Collection: "users",
			Document: map[string]interface{}{
				"name":  "John",
				"email": "john@example.com",
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		if sqlQuery.SQL == "" {
			t.Error("Expected SQL query, got empty string")
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test UPDATE query
	t.Run("UpdateQuery", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpUpdate,
			Collection: "users",
			Filters: []*query.Filter{
				{Field: "id", Operator: query.OpEqual, Value: 1},
			},
			Updates: map[string]interface{}{
				"name": "Jane",
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		if sqlQuery.SQL == "" {
			t.Error("Expected SQL query, got empty string")
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test DELETE query
	t.Run("DeleteQuery", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpDelete,
			Collection: "users",
			Filters: []*query.Filter{
				{Field: "id", Operator: query.OpEqual, Value: 1},
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		if sqlQuery.SQL == "" {
			t.Error("Expected SQL query, got empty string")
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test COUNT query
	t.Run("CountQuery", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpCount,
			Collection: "users",
			Filters: []*query.Filter{
				{Field: "status", Operator: query.OpEqual, Value: "active"},
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		if sqlQuery.SQL == "" {
			t.Error("Expected SQL query, got empty string")
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test complex query with multiple filters
	t.Run("ComplexQuery", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpFind,
			Collection: "users",
			Filters: []*query.Filter{
				{Field: "status", Operator: query.OpEqual, Value: "active"},
				{Field: "age", Operator: query.OpGreaterOrEqual, Value: 18},
			},
			Orders: []*query.Order{
				{Field: "created_at", Direction: "DESC"},
				{Field: "name", Direction: "ASC"},
			},
			Limit:  intPtr(20),
			Offset: intPtr(10),
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		if sqlQuery.SQL == "" {
			t.Error("Expected SQL query, got empty string")
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
		t.Logf("Args: %v", sqlQuery.Args)
	})

	// Test IN operator
	t.Run("InOperator", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpFind,
			Collection: "users",
			Filters: []*query.Filter{
				{
					Field:     "id",
					Operator:  query.OpIn,
					Values:    []interface{}{1, 2, 3},
				},
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test BETWEEN operator
	t.Run("BetweenOperator", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpFind,
			Collection: "users",
			Filters: []*query.Filter{
				{
					Field:     "age",
					Operator:  query.OpBetween,
					Values:    []interface{}{18, 65},
				},
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test NULL operator
	t.Run("NullOperator", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpFind,
			Collection: "users",
			Filters: []*query.Filter{
				{
					Field:    "deleted_at",
					Operator: query.OpNull,
				},
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})

	// Test LIKE operator
	t.Run("LikeOperator", func(t *testing.T) {
		q := &query.Query{
			Operation: query.OpFind,
			Collection: "users",
			Filters: []*query.Filter{
				{
					Field:    "name",
					Operator: query.OpLike,
					Value:    "John",
				},
			},
		}

		result, err := trans.Translate(q)
		if err != nil {
			t.Fatalf("Translation failed: %v", err)
		}

		sqlQuery, ok := result.(*translator.SQLQuery)
		if !ok {
			t.Fatalf("Expected SQLQuery, got %T", result)
		}

		t.Logf("Generated SQL: %s", sqlQuery.SQL)
	})
}

// TestPostgreSQLBindVar tests PostgreSQL bind variable format
func TestPostgreSQLBindVar(t *testing.T) {
	dialect := &postgres.PostgresDialect{}

	tests := []struct {
		index    int
		expected string
	}{
		{1, "$1"},
		{2, "$2"},
		{10, "$10"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := dialect.BindVar(tt.index)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// TestPostgreSQLQuoteIdentifier tests PostgreSQL identifier quoting
func TestPostgreSQLQuoteIdentifier(t *testing.T) {
	dialect := &postgres.PostgresDialect{}

	tests := []struct {
		input    string
		expected string
	}{
		{"users", `"users"`},
		{"user_data", `"user_data"`},
		{"Users", `"Users"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := dialect.QuoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}
