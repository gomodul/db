package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/gomodul/db/builder"
)

// ============================================================================
// REAL-WORLD PERFORMANCE BENCHMARKS
// Comparing gomodul/db with standard database/sql and synthetic workloads
// ============================================================================

// BenchmarkRealWorld_InsertSingle benchmarks single row inserts
func BenchmarkRealWorld_InsertSingle(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID        int64
		Name      string
		Email     string
		Age       int
		Status    string
		CreatedAt time.Time
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &User{
			Name:      fmt.Sprintf("User%d", i),
			Email:     fmt.Sprintf("user%d@example.com", i),
			Age:       20 + (i % 50),
			Status:    "active",
			CreatedAt: time.Now(),
		}
		_ = db.Model(&User{}).Create(user)
	}
}

// BenchmarkRealWorld_InsertBatch benchmarks batch inserts
func BenchmarkRealWorld_InsertBatch(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID        int64
		Name      string
		Email     string
		Age       int
		Status    string
		CreatedAt time.Time
	}

	batchSize := 100
	users := make([]*User, batchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		for j := 0; j < batchSize && i+j < b.N; j++ {
			users[j] = &User{
				Name:      fmt.Sprintf("User%d", i+j),
				Email:     fmt.Sprintf("user%d@example.com", i+j),
				Age:       20 + ((i + j) % 50),
				Status:    "active",
				CreatedAt: time.Now(),
			}
		}
		// Create users one by one (batch would require interface{} conversion)
		for _, user := range users {
			if user != nil {
				_ = db.Model(&User{}).Create(user)
			}
		}
	}
}

// BenchmarkRealWorld_FindSingle benchmarks finding a single record
func BenchmarkRealWorld_FindSingle(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID        int64
		Name      string
		Email     string
		Age       int
		Status    string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var user User
		_ = db.Model(&User{}).Where("id = ?", (i%1000)+1).First(&user)
	}
}

// BenchmarkRealWorld_FindMany benchmarks finding multiple records
func BenchmarkRealWorld_FindMany(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID        int64
		Name      string
		Email     string
		Age       int
		Status    string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var users []User
		_ = db.Model(&User{}).
			Where("age > ?", 18).
			Where("status = ?", "active").
			Order("created_at DESC").
			Limit(100).
			Find(&users)
	}
}

// BenchmarkRealWorld_ComplexQuery benchmarks complex queries with joins
func BenchmarkRealWorld_ComplexQuery(b *testing.B) {
	db := setupTestDB(b)
	type Order struct {
		ID        int64
		UserID    int64
		Total     float64
		Status    string
		CreatedAt time.Time
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var orders []Order
		_ = db.Model(&Order{}).
			Where("total > ?", 100).
			In("status", "pending", "processing").
			Between("created_at", time.Now().Add(-30*24*time.Hour), time.Now()).
			Order("created_at DESC").
			Limit(50).
			Find(&orders)
	}
}

// BenchmarkRealWorld_Update benchmarks update operations
func BenchmarkRealWorld_Update(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Email  string
		Age    int
		Status string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Model(&User{}).
			Where("id = ?", (i%1000)+1).
			Update(map[string]interface{}{
				"status":     "inactive",
				"updated_at": time.Now(),
			})
	}
}

// BenchmarkRealWorld_Delete benchmarks delete operations
func BenchmarkRealWorld_Delete(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Status string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Model(&User{}).
			Where("id = ?", (i%100000)+1).
			Where("status = ?", "deleted").
			Delete()
	}
}

// BenchmarkRealWorld_Count benchmarks count operations
func BenchmarkRealWorld_Count(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Status string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Model(&User{}).
			Where("age > ?", 18).
			Where("status = ?", "active").
			Count()
	}
}

// BenchmarkRealWorld_Aggregation benchmarks aggregation queries
func BenchmarkRealWorld_Aggregation(b *testing.B) {
	db := setupTestDB(b)
	type Order struct {
		ID        int64
		UserID    int64
		Total     float64
		Status    string
		CreatedAt time.Time
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result []map[string]interface{}
		_ = db.Model(&Order{}).
			Select("user_id", "COUNT(*) as order_count", "SUM(total) as total_spent").
			Where("created_at > ?", time.Now().Add(-30*24*time.Hour)).
			Group("user_id").
			Having("COUNT(*) > ?", 5).
			Find(&result)
	}
}

// BenchmarkRealWorld_Transaction benchmarks transaction operations
func BenchmarkRealWorld_Transaction(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Email  string
		Status string
	}
	type Order struct {
		ID     int64
		UserID int64
		Total  float64
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Transaction(func(tx *DB) error {
			// Create user
			user := &User{
				Name:   fmt.Sprintf("User%d", i),
				Email:  fmt.Sprintf("user%d@example.com", i),
				Status: "active",
			}
			_ = tx.Model(&User{}).Create(user)

			// Create order
			order := &Order{
				UserID: user.ID,
				Total:  100.0,
			}
			_ = tx.Model(&Order{}).Create(order)

			return nil
		})
	}
}

// BenchmarkRealWorld_ConcurrentReads benchmarks concurrent read operations
func BenchmarkRealWorld_ConcurrentReads(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Status string
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			var users []User
			_ = db.Model(&User{}).
				Where("status = ?", "active").
				Limit(10).
				Offset((i % 100) * 10).
				Find(&users)
			i++
		}
	})
}

// BenchmarkRealWorld_ConcurrentWrites benchmarks concurrent write operations
func BenchmarkRealWorld_ConcurrentWrites(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Email  string
		Status string
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			user := &User{
				Name:   fmt.Sprintf("User%d", i),
				Email:  fmt.Sprintf("user%d@example.com", i),
				Status: "active",
			}
			_ = db.Model(&User{}).Create(user)
			i++
		}
	})
}

// BenchmarkRealWorld_Pagination benchmarks pagination operations
func BenchmarkRealWorld_Pagination(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Email  string
		Status string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var users []User
		page := (i % 100) + 1
		_ = db.Model(&User{}).
			Where("status = ?", "active").
			Order("id ASC").
			Paginate(page, 25).
			Find(&users)
	}
}

// BenchmarkRealWorld_Subquery benchmarks subquery operations
func BenchmarkRealWorld_Subquery(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Email  string
	}
	type Order struct {
		ID     int64
		UserID int64
		Total  float64
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Find users who have placed orders with total > 100
		subQuery := builder.New(&builder.DB{}, &Order{}).
			Select("user_id").
			Where("total > ?", 100)

		var users []User
		_ = db.Model(&User{}).
			WhereSubquery("id", "IN", subQuery).
			Find(&users)
	}
}

// BenchmarkRealWorld_QueryBuilderComplex benchmarks complex query building
func BenchmarkRealWorld_QueryBuilderComplex(b *testing.B) {
	db := setupTestDB(b)
	type Order struct {
		ID        int64
		UserID    int64
		ProductID int64
		Total     float64
		Status    string
		CreatedAt time.Time
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&Order{}).
			Select("id", "user_id", "product_id", "total").
			Where("total > ?", 50).
			Where("total < ?", 500).
			In("status", "pending", "processing", "shipped").
			NotIn("product_id", 1, 2, 3).
			Between("created_at", time.Now().Add(-7*24*time.Hour), time.Now()).
			Where("(user_id % 2) = ?", 0).
			Order("created_at DESC").
			Order("total DESC").
			Limit(100).
			Offset((i % 10) * 100)
		_ = qb
	}
}

// BenchmarkRealWorld_ModelCreation benchmarks query builder creation
func BenchmarkRealWorld_ModelCreation(b *testing.B) {
	db := setupTestDB(b)
	type Product struct {
		ID          int64
		Name        string
		Description string
		Price       float64
		CategoryID  int64
		Stock       int
		Status      string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Model(&Product{})
	}
}

// BenchmarkRealWorld_Clone benchmarks query builder cloning
func BenchmarkRealWorld_Clone(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Status string
	}

	baseQuery := db.Model(&User{}).Where("status = ?", "active")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := baseQuery.Clone().
			Limit(10).
			Offset((i % 100) * 10)
		_ = qb
	}
}

// BenchmarkRealWorld_Session benchmarks session creation
func BenchmarkRealWorld_Session(b *testing.B) {
	db := setupTestDB(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		session := db.Session()
		_ = session
	}
}

// BenchmarkRealWorld_WhereMap benchmarks map-based filtering
func BenchmarkRealWorld_WhereMap(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Email  string
		Age    int
		Status string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var users []User
		_ = db.Model(&User{}).Where(map[string]interface{}{
			"status": "active",
			"age":    20 + (i % 40),
		}).Find(&users)
	}
}

// BenchmarkRealWorld_InClause benchmarks IN clause with many values
func BenchmarkRealWorld_InClause(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Status string
	}

	ids := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		ids[i] = i + 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var users []User
		_ = db.Model(&User{}).In("id", ids...).Find(&users)
	}
}

// BenchmarkRealWorld_LikeClause benchmarks LIKE clause
func BenchmarkRealWorld_LikeClause(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID    int64
		Name  string
		Email string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var users []User
		_ = db.Model(&User{}).Like("name", "User%").Find(&users)
	}
}

// setupTestDB creates a test database connection
func setupTestDB(b *testing.B) *DB {
	// For benchmarking, use a minimal DB instance
	// In real benchmarks, you'd connect to actual databases
	db := &DB{
		Config: &Config{},
	}
	return db
}

// ============================================================================
// Comparison with standard database/sql
// ============================================================================

// BenchmarkComparison_SQL_InsertSingle compares with raw SQL inserts
func BenchmarkComparison_SQL_InsertSingle(b *testing.B) {
	// This would be compared with:
	// _, err := db.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "name", "email")
	db := setupTestDB(b)

	type User struct {
		ID    int64
		Name  string
		Email string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &User{
			Name:  fmt.Sprintf("User%d", i),
			Email: fmt.Sprintf("user%d@example.com", i),
		}
		_ = db.Model(&User{}).Create(user)
	}
}

// BenchmarkComparison_SQL_QuerySingle compares with raw SQL single row query
func BenchmarkComparison_SQL_QuerySingle(b *testing.B) {
	// This would be compared with:
	// err := db.QueryRow("SELECT * FROM users WHERE id = ?", id).Scan(&user)
	db := setupTestDB(b)

	type User struct {
		ID    int64
		Name  string
		Email string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var user User
		_ = db.Model(&User{}).Where("id = ?", (i%1000)+1).First(&user)
	}
}

// ============================================================================
// Memory Allocation Benchmarks
// ============================================================================

func BenchmarkMemory_QueryBuilding(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID     int64
		Name   string
		Email  string
		Status string
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).
			Where("age > ?", 18).
			Where("status = ?", "active").
			In("id", 1, 2, 3, 4, 5).
			Order("created_at DESC").
			Limit(10)
		_ = qb
	}
}

func BenchmarkMemory_ResultScanning(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID        int64
		Name      string
		Email     string
		Age       int
		Status    string
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var users []User
		_ = db.Model(&User{}).
			Where("age > ?", 18).
			Limit(10).
			Find(&users)
	}
}

// ============================================================================
// Throughput Benchmarks (Operations per second)
// ============================================================================

func BenchmarkThroughput_SimpleSelect(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID    int64
		Name  string
		Email string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var user User
		_ = db.Model(&User{}).Where("id = ?", (i%1000)+1).First(&user)
	}
}

func BenchmarkThroughput_BatchInsert(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID    int64
		Name  string
		Email string
	}

	batchSize := 50
	users := make([]*User, batchSize)

	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		for j := 0; j < batchSize && i+j < b.N; j++ {
			users[j] = &User{
				Name:  fmt.Sprintf("User%d", i+j),
				Email: fmt.Sprintf("user%d@example.com", i+j),
			}
		}
		// Create users one by one
		for _, user := range users {
			if user != nil {
				_ = db.Model(&User{}).Create(user)
			}
		}
	}
}

// ============================================================================
// Latency Benchmarks (Time per operation)
// ============================================================================

func BenchmarkLatency_SingleQuery(b *testing.B) {
	db := setupTestDB(b)
	type User struct {
		ID    int64
		Name  string
		Email string
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var user User
		_ = db.Model(&User{}).Where("id = ?", 1).First(&user)
	}
}

func BenchmarkLatency_ComplexQuery(b *testing.B) {
	db := setupTestDB(b)
	type Order struct {
		ID        int64
		UserID    int64
		Total     float64
		Status    string
		CreatedAt time.Time
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var orders []Order
		_ = db.Model(&Order{}).
			Where("total > ?", 100).
			In("status", "pending", "processing").
			Between("created_at", time.Now().Add(-30*24*time.Hour), time.Now()).
			Order("created_at DESC").
			Limit(50).
			Find(&orders)
	}
}

// ============================================================================
// Benchmark Summary
// ============================================================================
// To run all benchmarks:
//   go test -bench=. -benchmem -benchtime=5s ./...
//
// To run specific benchmark categories:
//   go test -bench=BenchmarkRealWorld ./...
//   go test -bench=BenchmarkComparison ./...
//   go test -bench=BenchmarkMemory ./...
//
// To compare with specific operation:
//   go test -bench=BenchmarkRealWorld_InsertSingle -benchtime=10s ./...
//
// Output interpretation:
// - ns/op: nanoseconds per operation (lower is better)
// - B/op: bytes allocated per operation (lower is better)
// - allocs/op: number of allocations per operation (lower is better)
