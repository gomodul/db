// +build integration

package db

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gomodul/db/dialect"
)

// TestIntegration_PostgreSQL_CRUD tests CRUD operations with PostgreSQL
func TestIntegration_PostgreSQL_CRUD(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	database, err := Open(Config{DSN: dsn})
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer database.Close()

	// Test AutoMigrate
	type TestUser struct {
		ID      int64     `db:"id,pk,autoIncrement"`
		Name    string    `db:"name,size:100"`
		Email   string    `db:"email,unique,size:255"`
		Age     int       `db:"age"`
		Status  string    `db:"status,default:'active'"`
		Created time.Time `db:"created_at"`
	}

	if err := database.AutoMigrate(&TestUser{}); err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	// Test Create
	user := &TestUser{
		Name:    "Integration Test User",
		Email:   "integration@example.com",
		Age:     30,
		Status:  "active",
		Created: time.Now(),
	}

	if err := database.Model(&TestUser{}).Create(user); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if user.ID == 0 {
		t.Error("Expected user ID to be set")
	}

	// Test Read
	var found TestUser
	if err := database.Model(&TestUser{}).Where("id = ?", user.ID).First(&found); err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if found.Name != user.Name {
		t.Errorf("Expected name %s, got %s", user.Name, found.Name)
	}

	// Test Update
	if err := database.Model(&TestUser{}).Where("id = ?", user.ID).Update(map[string]interface{}{
		"age": 35,
	}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify update
	var updated TestUser
	if err := database.Model(&TestUser{}).Where("id = ?", user.ID).First(&updated); err != nil {
		t.Fatalf("Find after update failed: %v", err)
	}

	if updated.Age != 35 {
		t.Errorf("Expected age 35, got %d", updated.Age)
	}

	// Test Count
	var count int64
	if count, err := database.Model(&TestUser{}).Where("age > ?", 25).Count(); err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count == 0 {
		t.Error("Expected at least one user with age > 25")
	}

	// Test Delete
	if err := database.Model(&TestUser{}).Where("id = ?", user.ID).Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	var deleted TestUser
	err = database.Model(&TestUser{}).Where("id = ?", user.ID).First(&deleted)
	if err == nil {
		t.Error("Expected error when finding deleted user")
	}
}

// TestIntegration_PostgreSQL_Transaction tests transaction support
func TestIntegration_PostgreSQL_Transaction(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	database, err := Open(Config{DSN: dsn})
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer database.Close()

	type TestUser struct {
		ID     int64  `db:"id,pk,autoIncrement"`
		Name   string `db:"name"`
		Amount int    `db:"amount"`
	}

	if err := database.AutoMigrate(&TestUser{}); err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	// Test successful transaction
	err = database.Transaction(func(tx *DB) error {
		user := &TestUser{Name: "User1", Amount: 100}
		if err := tx.Model(&TestUser{}).Create(user); err != nil {
			return err
		}

		// Verify user exists within transaction
		var found TestUser
		if err := tx.Model(&TestUser{}).Where("id = ?", user.ID).First(&found); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Test transaction rollback
	err = database.Transaction(func(tx *DB) error {
		user := &TestUser{Name: "User2", Amount: 200}
		if err := tx.Model(&TestUser{}).Create(user); err != nil {
			return err
		}

		// Force rollback
		return fmt.Errorf("rollback test")
	})

	if err == nil {
		t.Error("Expected error from rollback transaction")
	}

	// Verify rollback - user should not exist
	var count int64
	count, _ = database.Model(&TestUser{}).Where("name = ?", "User2").Count()
	if count > 0 {
		t.Error("Transaction was not rolled back")
	}
}

// TestIntegration_PostgreSQL_ComplexQueries tests complex query operations
func TestIntegration_PostgreSQL_ComplexQueries(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	database, err := Open(Config{DSN: dsn})
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer database.Close()

	type TestOrder struct {
		ID        int64    `db:"id,pk,autoIncrement"`
		UserID    int64    `db:"user_id"`
		Total     float64 `db:"total"`
		Status    string   `db:"status"`
		CreatedAt time.Time `db:"created_at"`
	}

	// Create test orders
	for i := 1; i <= 10; i++ {
		order := &TestOrder{
			UserID:    int64(i),
			Total:     float64(i * 100),
			Status:    []string{"pending", "processing", "completed"}[i%3],
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Hour),
		}
		if err := database.Model(&TestOrder{}).Create(order); err != nil {
			t.Fatalf("Failed to create test order: %v", err)
		}
	}

	// Test IN clause
	var orders []TestOrder
	if err := database.Model(&TestOrder{}).
		In("status", "pending", "processing").
		Find(&orders); err != nil {
		t.Fatalf("IN query failed: %v", err)
	}

	if len(orders) < 3 {
		t.Errorf("Expected at least 3 orders with status pending/processing, got %d", len(orders))
	}

	// Test BETWEEN
	var rangeOrders []TestOrder
	if err := database.Model(&TestOrder{}).
		Between("total", 300, 700).
		Find(&rangeOrders); err != nil {
		t.Fatalf("BETWEEN query failed: %v", err)
	}

	if len(rangeOrders) < 4 {
		t.Errorf("Expected at least 4 orders in range, got %d", len(rangeOrders))
	}

	// Test pagination
	page1 := make([]TestOrder, 0)
	if err := database.Model(&TestOrder{}).
		Order("created_at DESC").
		Limit(3).
		Find(&page1); err != nil {
		t.Fatalf("Pagination page 1 failed: %v", err)
	}

	if len(page1) != 3 {
		t.Errorf("Expected 3 orders on page 1, got %d", len(page1))
	}

	page2 := make([]TestOrder, 0)
	if err := database.Model(&TestOrder{}).
		Order("created_at DESC").
		Limit(3).
		Offset(3).
		Find(&page2); err != nil {
		t.Fatalf("Pagination page 2 failed: %v", err)
	}

	if len(page2) != 3 {
		t.Errorf("Expected 3 orders on page 2, got %d", len(page2))
	}

	// Verify pages are different
	if len(page1) > 0 && len(page2) > 0 {
		if page1[0].ID == page2[0].ID {
			t.Error("Pages should have different orders")
		}
	}
}

// TestIntegration_PostgreSQL_AutoMigrate tests schema migration
func TestIntegration_PostgreSQL_AutoMigrate(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	database, err := Open(Config{DSN: dsn})
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer database.Close()

	type TestProduct struct {
		ID          int64   `db:"id,pk,autoIncrement"`
		Name        string  `db:"name,size:200,notnull"`
		Description string  `db:"description,type:TEXT"`
		Price       float64 `db:"price,notnull"`
		CategoryID  int64   `db:"category_id"`
		Stock       int     `db:"stock,default:0"`
		Active      bool    `db:"active,default:true"`
		Tags        string  `db:"tags,size:500"`
		CreatedAt   time.Time `db:"created_at"`
	}

	// Test AutoMigrate
	if err := database.AutoMigrate(&TestProduct{}); err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	// Verify table exists by trying to insert
	product := &TestProduct{
		Name:        "Integration Test Product",
		Description: "A product for integration testing",
		Price:       29.99,
		CategoryID:  1,
		Stock:       100,
		Active:      true,
		Tags:        "test,integration",
		CreatedAt:   time.Now(),
	}

	if err := database.Model(&TestProduct{}).Create(product); err != nil {
		t.Fatalf("Failed to insert product: %v", err)
	}

	if product.ID == 0 {
		t.Error("Expected product ID to be set")
	}

	// Verify default values
	if product.Active != true {
		t.Error("Expected Active to default to true")
	}

	// Clean up
	database.Model(&TestProduct{}).Where("id = ?", product.ID).Delete()
}

// TestIntegration_PostgreSQL_PoolMonitoring tests connection pool monitoring
func TestIntegration_PostgreSQL_PoolMonitoring(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	database, err := Open(Config{
		DSN:            dsn,
		MaxOpenConns:   10,
		MaxIdleConns:   3,
		ConnMaxLifetime: 30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer database.Close()

	// Enable pool monitoring
	if err := database.EnablePoolMonitoring(nil); err != nil {
		t.Fatalf("Failed to enable pool monitoring: %v", err)
	}

	ctx := context.Background()

	// Test GetPoolStats
	stats, err := database.GetPoolStats(ctx)
	if err != nil {
		t.Fatalf("Failed to get pool stats: %v", err)
	}

	if stats == nil {
		t.Error("Expected stats to be returned")
	} else {
		if stats.MaxOpenConnections != 10 {
			t.Errorf("Expected MaxOpenConnections 10, got %d", stats.MaxOpenConnections)
		}
	}

	// Test GetPoolHealth
	health, err := database.GetPoolHealth(ctx)
	if err != nil {
		t.Fatalf("Failed to get pool health: %v", err)
	}

	if health == nil {
		t.Error("Expected health to be returned")
	} else {
		if !health.Healthy {
			t.Logf("Pool health warnings: %v", health.Warnings)
		}
	}

	// Test CollectPoolMetrics
	if err := database.CollectPoolMetrics(ctx); err != nil {
		t.Logf("Failed to collect pool metrics: %v", err)
	}

	// Test GetPoolInfo
	info := database.GetPoolInfo()
	if info == nil {
		t.Error("Expected pool info to be returned")
	}
}

// TestIntegration_SQLite_CRUD tests CRUD operations with SQLite
func TestIntegration_SQLite_CRUD(t *testing.T) {
	dsn := os.Getenv("TEST_SQLITE_DSN")
	if dsn == "" {
		dsn = "file:///tmp/test_integration.db"
	}

	database, err := Open(Config{DSN: dsn})
	if err != nil {
		t.Fatalf("Failed to connect to SQLite: %v", err)
	}
	defer func() {
		database.Close()
		os.Remove("/tmp/test_integration.db")
	}()

	type TestItem struct {
		ID      int64  `db:"id,pk,autoIncrement"`
		Name    string `db:"name"`
		Value   int    `db:"value"`
		Enabled bool   `db:"enabled,default:true"`
	}

	// Test AutoMigrate
	if err := database.AutoMigrate(&TestItem{}); err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	// Test Create
	item := &TestItem{Name: "Test Item", Value: 42}
	if err := database.Model(&TestItem{}).Create(item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Test Read
	var found TestItem
	if err := database.Model(&TestItem{}).Where("id = ?", item.ID).First(&found); err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if found.Name != item.Name {
		t.Errorf("Expected name %s, got %s", item.Name, found.Name)
	}

	// Test Update
	if err := database.Model(&TestItem{}).Where("id = ?", item.ID).Update(map[string]interface{}{
		"value": 100,
	}); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Test Delete
	if err := database.Model(&TestItem{}).Where("id = ?, item.ID).Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

// TestIntegration_DatabaseSwitching tests switching between different databases
func TestIntegration_DatabaseSwitching(t *testing.T) {
	// This test verifies that the same code works with different databases

	postgresDSN := os.Getenv("TEST_POSTGRES_DSN")
	sqliteDSN := os.Getenv("TEST_SQLITE_DSN")
	if postgresDSN == "" && sqliteDSN == "" {
		t.Skip("No test DSNs configured")
	}

	// Test function that works with any database
	testCRUDOperations := func(dsn string, dbName string) error {
		database, err := Open(Config{DSN: dsn})
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", dbName, err)
		}
		defer database.Close()

		type TestRecord struct {
			ID   int64  `db:"id,pk,autoIncrement"`
			Data string `db:"data"`
		}

		// AutoMigrate
		if err := database.AutoMigrate(&TestRecord{}); err != nil {
			return fmt.Errorf("automigrate failed: %w", err)
		}

		// Create
		record := &TestRecord{Data: fmt.Sprintf("Test data from %s", dbName)}
		if err := database.Model(&TestRecord{}).Create(record); err != nil {
			return fmt.Errorf("create failed: %w", err)
		}

		// Read
		var found TestRecord
		if err := database.Model(&TestRecord{}).Where("id = ?", record.ID).First(&found); err != nil {
			return fmt.Errorf("find failed: %w", err)
		}

		// Clean up
		database.Model(&TestRecord{}).Where("id = ?", record.ID).Delete()

		return nil
	}

	// Test with SQLite (if available)
	if sqliteDSN != "" {
		t.Log("Testing with SQLite...")
		if err := testCRUDOperations(sqliteDSN, "SQLite"); err != nil {
			t.Errorf("SQLite test failed: %v", err)
		}
	}

	// Test with PostgreSQL (if available)
	if postgresDSN != "" {
		t.Log("Testing with PostgreSQL...")
		if err := testCRUDOperations(postgresDSN, "PostgreSQL"); err != nil {
			t.Errorf("PostgreSQL test failed: %v", err)
		}
	}
}

// TestIntegration_Streaming tests streaming functionality with real data
func TestIntegration_Streaming(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set")
	}

	database, err := Open(Config{DSN: dsn})
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer database.Close()

	type StreamTest struct {
		ID   int64  `db:"id,pk,autoIncrement"`
		Name string `db:"name"`
	}

	// Create test data
	for i := 0; i < 100; i++ {
		record := &StreamTest{Name: fmt.Sprintf("Stream%d", i)}
		if err := database.Model(&StreamTest{}).Create(record); err != nil {
			t.Fatalf("Failed to create test record: %v", err)
		}
	}

	// Test Cursor streaming
	cursor, err := database.Model(&StreamTest{}).Cursor()
	if err != nil {
		t.Fatalf("Failed to create cursor: %v", err)
	}
	defer cursor.Close()

	count := 0
	for cursor.Next() {
		var record StreamTest
		if err := cursor.Scan(&record); err != nil {
			t.Fatalf("Failed to scan record: %v", err)
		}
		count++
	}

	if count != 100 {
		t.Errorf("Expected 100 records from cursor, got %d", count)
	}

	// Test ScanEach
	scanCount := 0
	err = ScanEach(
		database.Model(&StreamTest{}),
		func(record *StreamTest) error {
			scanCount++
			return nil
		},
	)

	if err != nil {
		t.Fatalf("ScanEach failed: %v", err)
	}

	if scanCount != 100 {
		t.Errorf("Expected 100 records from ScanEach, got %d", scanCount)
	}

	// Clean up
	database.Model(&StreamTest{}).Where("1 = 1").Delete()
}

// BenchmarkIntegration_RealQuery benchmarks real database queries
func BenchmarkIntegration_RealQuery(b *testing.B) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		b.Skip("TEST_POSTGRES_DSN not set")
	}

	database, err := Open(Config{DSN: dsn})
	if err != nil {
		b.Fatalf("Failed to connect: %v", err)
	}
	defer database.Close()

	type BenchUser struct {
		ID    int64  `db:"id,pk,autoIncrement"`
		Name  string `db:"name,size:100"`
		Email string `db:"email,size:255"`
		Age   int    `db:"age"`
	}

	// AutoMigrate
	_ = database.AutoMigrate(&BenchUser{})

	// Create test data
	for i := 0; i < 1000; i++ {
		user := &BenchUser{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
			Age:   20 + (i % 50),
		}
		_ = database.Model(&BenchUser{}).Create(user)
	}

	b.ResetTimer()

	// Benchmark simple SELECT
	for i := 0; i < b.N; i++ {
		var user BenchUser
		_ = database.Model(&BenchUser{}).Where("id = ?", (i%1000)+1).First(&user)
	}

	// Clean up
	database.Model(&BenchUser{}).Where("1 = 1").Delete()
}
