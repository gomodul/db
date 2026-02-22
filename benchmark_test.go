package db

import (
	"testing"
	"time"

	"github.com/gomodul/db/internal/security"
)

// BenchmarkQueryBuilder_Where benchmarks Where clause building
func BenchmarkQueryBuilder_Where(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID   int64
		Name string
		Age  int
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{})
		qb.Where("age > ?", 18)
		qb.Where("status = ?", "active")
		_ = qb
	}
}

// BenchmarkQueryBuilder_WhereMap benchmarks Where with map filter
func BenchmarkQueryBuilder_WhereMap(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID     int64
		Name   string
			Age    int
		Status string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{})
		qb.Where(map[string]interface{}{
			"age":    18,
			"status": "active",
		})
		_ = qb
	}
}

// BenchmarkQueryBuilder_In benchmarks In clause
func BenchmarkQueryBuilder_In(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID int64
	}

	values := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).In("id", values...)
		_ = qb
	}
}

// BenchmarkQueryBuilder_Between benchmarks Between clause
func BenchmarkQueryBuilder_Between(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID        int64
		CreatedAt time.Time
	}

	start := time.Now()
	end := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).Between("created_at", start, end)
		_ = qb
	}
}

// BenchmarkQueryBuilder_Chain benchmarks chained queries
func BenchmarkQueryBuilder_Chain(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID        int64
		Name      string
	age       int
		Status    string
		CreatedAt time.Time
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).
			Where("age > ?", 18).
			Where("status = ?", "active").
			In("id", 1, 2, 3).
			Between("created_at", time.Now(), time.Now()).
			Order("created_at DESC").
			Limit(10).
			Offset(20)
		_ = qb
	}
}

// BenchmarkQueryBuilder_Limit benchmarks Limit clause
func BenchmarkQueryBuilder_Limit(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID int64
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).Limit(100)
		_ = qb
	}
}

// BenchmarkQueryBuilder_Order benchmarks Order clause
func BenchmarkQueryBuilder_Order(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID        int64
		Name      string
		CreatedAt time.Time
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).Order("created_at DESC").Order("name ASC")
		_ = qb
	}
}

// BenchmarkQueryBuilder_Join benchmarks Join operations
func BenchmarkQueryBuilder_Join(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID        int64
		ProfileID int64
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).
			Join("profiles", "profiles.id = users.profile_id").
			LeftJoin("posts", "posts.user_id = users.id")
		_ = qb
	}
}

// BenchmarkQueryBuilder_Paginate benchmarks pagination
func BenchmarkQueryBuilder_Paginate(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID int64
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).Paginate(i%10+1, 25)
		_ = qb
	}
}

// BenchmarkSecurity_ValidateRawQuery benchmarks raw query validation
func BenchmarkSecurity_ValidateRawQuery(b *testing.B) {
	cfg := security.DefaultRawQueryConfig()
	sql := "SELECT * FROM users WHERE id = ? AND status = ?"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = security.ValidateRawQuery(sql, cfg)
	}
}

// BenchmarkSecurity_ValidateFieldName benchmarks field name validation
func BenchmarkSecurity_ValidateFieldName(b *testing.B) {
	validFields := []string{
		"username", "user_name", "profile.name", "id", "created_at",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = security.ValidateFieldName(validFields[i%len(validFields)])
	}
}

// BenchmarkSecurity_SanitizeFieldName benchmarks field name sanitization
func BenchmarkSecurity_SanitizeFieldName(b *testing.B) {
	inputs := []string{
		"username", "user;DROP TABLE", "1field", "profile.name",
		"user name", "id",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = security.SanitizeFieldName(inputs[i%len(inputs)])
	}
}

// BenchmarkOpen_Close benchmarks database open and close
func BenchmarkOpen_Close(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := Open(Config{DSN: "sqlite::file:"})
		if err != nil {
			continue
		}
		db.Close()
	}
}

// BenchmarkModel benchmarks Model creation
func BenchmarkModel(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID   int64
		Name string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Model(&User{})
	}
}

// BenchmarkSession benchmarks Session creation
func BenchmarkSession(b *testing.B) {
	db := &DB{Config: &Config{}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Session()
	}
}

// BenchmarkQueryBuilder_Clone benchmarks QueryBuilder cloning
func BenchmarkQueryBuilder_Clone(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID   int64
		Name string
	}

	qb := db.Model(&User{}).Where("age > ?", 18)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = qb.Clone()
	}
}

// Parallel benchmarks
func BenchmarkQueryBuilder_ParallelWhere(b *testing.B) {
	database := &DB{Config: &Config{}}
	type User struct {
		ID   int64
		Name string
		Age  int
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			qb := database.Model(&User{}).Where("age > ?", 18)
			_ = qb
		}
	})
}

func BenchmarkQueryBuilder_ParallelChain(b *testing.B) {
	database := &DB{Config: &Config{}}
	type User struct {
		ID        int64
		Name      string
		age       int
		Status    string
		CreatedAt time.Time
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			qb := database.Model(&User{}).
				Where("age > ?", 18).
				Where("status = ?", "active").
				Order("created_at DESC").
				Limit(10)
			_ = qb
		}
	})
}

// Memory allocation benchmarks
func BenchmarkQueryBuilder_Memory(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID        int64
		Name      string
		Email     string
	age       int
		Status    string
		CreatedAt time.Time
		UpdatedAt time.Time
	}

	b.Run("SingleQuery", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			qb := db.Model(&User{}).
				Where("id = ?", i).
				First(&User{})
			_ = qb
		}
	})

	b.Run("ComplexQuery", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			qb := db.Model(&User{}).
				Where("age > ?", 18).
				In("id", 1, 2, 3, 4, 5).
				Between("created_at", time.Now(), time.Now()).
				Order("created_at DESC").
				Limit(10).
				Offset(20)
			_ = qb
		}
	})
}

// Benchmark allocation of large slices
func BenchmarkQueryBuilder_LargeSlice(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID int64
	}

	largeSlice := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		largeSlice[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).In("id", largeSlice...)
		_ = qb
	}
}
