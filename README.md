# gomodul/db - Universal Database Abstraction Layer for Go

<div align="center">

**Write your repository code ONCE, use it with ANY backend!**

[![Go Report Card](https://goreportcard.com/badge/github.com/gomodul/db)](https://goreportcard.com/report/github.com/gomodul/db)
[![GoDoc](https://pkg.go.dev/badge/github.com/gomodul/db)](https://pkg.go.dev/github.com/gomodul/db)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**⚡ The most flexible, secure, and performant database library for Go**

</div>

## ⭐ Features

- **🔄 Universal API** - One codebase works with 11+ database backends
- **🔒 Secure by Default** - Built-in SQL injection protection and query validation
- **⚡ Blazing Fast** - Prepared statement caching, connection pooling, query caching
- **🎯 Fluent Interface** - Clean, chainable API inspired by GORM
- **📊 Streaming Support** - Process millions of records without memory issues
- **🔍 Advanced Queries** - CTE, subquery, window functions, GORM-style dynamic detection
- **🛡️ Enterprise Ready** - Transactions, retry logic, health checks, metrics

## 🎯 Goal

`gomodul/db` allows you to write your repository code **once** and seamlessly switch between **11 different backends** by only changing the connection DSN - no code modifications needed!

### Supported Backends

| Type | Driver | Import | Status |
|------|--------|--------|--------|
| **SQL** | PostgreSQL | `github.com/gomodul/db/driver/postgres` | ✅ Full Support |
| **SQL** | MySQL | `github.com/gomodul/db/driver/mysql` | ✅ Full Support |
| **SQL** | SQLite | `github.com/gomodul/db/driver/sqlite` | ✅ Full Support |
| **NoSQL** | MongoDB | `github.com/gomodul/db/driver/mongodb` | ✅ Full Support |
| **NoSQL** | Redis | `github.com/gomodul/db/driver/redis` | ✅ Full Support |
| **Search** | Elasticsearch | `github.com/gomodul/db/driver/elasticsearch` | ✅ Full Support |
| **API** | REST | `github.com/gomodul/db/driver/rest` | ✅ Full Support |
| **API** | GraphQL | `github.com/gomodul/db/driver/graphql` | ✅ Full Support |
| **RPC** | gRPC | `github.com/gomodul/db/driver/grpc` | ✅ Full Support |
| **MQ** | Kafka | `github.com/gomodul/db/driver/kafka` | ✅ Full Support |

## 🚀 Quick Start

### Installation

```bash
go get github.com/gomodul/db
```

### Basic Usage

```go
package main

import (
    "log"
    "github.com/gomodul/db"
    _ "github.com/gomodul/db/driver/postgres" // Import driver
)

type User struct {
    ID       int64  `json:"id" db:"id,pk"`
    Name     string `json:"name" db:"name"`
    Email    string `json:"email" db:"email"`
    Age      int    `json:"age" db:"age"`
    Status   string `json:"status" db:"status"`
}

func main() {
    // Open database connection
    database, err := db.Open(db.Config{
        DSN: "postgres://user:pass@localhost:5432/mydb",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer database.Close()

    // GORM-style dynamic detection
    var users []User
    database.Model(&User{}).
        Where("age > ?", 18).                    // Comparison
        Where("id IN ?", []int{1,2,3}).          // IN from slice
        Where("status = ?", "active").             // String equality
        Order("created_at DESC").
        Limit(10).
        Find(&users)

    // Create
    user := &User{Name: "John", Age: 30, Status: "active"}
    database.Model(&User{}).Create(user)
}
```

## 📖 Documentation

### ⚡ NEW: GORM-Style Dynamic Where

```go
// Auto-detects operator from value type
db.Where("id IN ?", []int{1,2,3})              // IN clause from slice
db.Where("age BETWEEN ? AND ?", 18, 65)        // BETWEEN
db.Where("name LIKE ?", "John%")                // LIKE

// Map filter (AND logic)
db.Where(map[string]interface{}{
    "status": "active",
    "age": 30,
})

// Struct filter (zero values ignored)
db.Where(User{Name: "John", Age: 30})
```

### 🎯 Shortcuts for Common Clauses

```go
// IN / NOT IN (shorter, cleaner)
db.In("id", 1, 2, 3)
db.NotIn("id", 4, 5, 6)

// BETWEEN
db.Between("created_at", startTime, endTime)

// NULL checks
db.Null("deleted_at")
db.NotNull("verified_at")

// LIKE
db.Like("name", "John%")
db.NotLike("email", "%@spam.com")
```

### 🗄️ AutoMigrate - Automatic Schema Management

Automatically create and update database tables based on your Go structs:

```go
type User struct {
    ID        int64     `json:"id" db:"id,pk,autoIncrement"`
    Name      string    `json:"name" db:"name,size:100"`
    Email     string    `json:"email" db:"email,unique,size:255"`
    Age       int       `json:"age" db:"age"`
    Status    string    `json:"status" db:"status,default:'active'"`
    CreatedAt time.Time `json:"created_at" db:"created_at"`
    UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// Simple auto-migration
err := database.AutoMigrate(&User{})

// Migrate multiple models
err := database.AutoMigrate(&User{}, &Order{}, &Product{})
```

**Available DB Tags:**
- `pk` - Primary key
- `autoIncrement` - Auto-incrementing column
- `unique` - Unique constraint
- `index` - Create index
- `size:N` - String size (e.g., `size:255`)
- `notnull` - NOT NULL constraint
- `default:value` - Default value

**Advanced Migration Operations:**

```go
migrator := database.Migrator()

// Create custom indexes
migrator.CreateIndex(ctx, &User{}, &migrate.IndexInfo{
    Name:    "idx_user_email",
    Columns: []string{"email"},
    Unique:  true,
})

// Drop table
migrator.DropTable(ctx, &User{})

// Check if table/column exists
hasTable, _ := migrator.HasTable(ctx, "users")
hasColumn, _ := migrator.HasColumn(ctx, "users", "email")
```

### 🚀 Advanced Query Features (CTE, Subquery, Window Functions)

```go
// Common Table Expressions (WITH clause)
db.Model(&Order{}).
    WithCTE("active_users", `
        SELECT id, name FROM users WHERE status = 'active'
    `).
    Where("orders.user_id IN (SELECT id FROM active_users)")

// Or use a subquery builder for CTE
userCTE := db.Model(&User{}).Where("status = ?", "active")
db.Model(&Order{}).
    WithCTE("active_users", userCTE).
    Join("active_users", "active_users.id = orders.user_id")

// Recursive CTE for hierarchical data
db.Model(&Category{}).
    WithRecursiveCTE("category_tree", `
        SELECT id, name, parent_id FROM categories WHERE parent_id IS NULL
        UNION ALL
        SELECT c.id, c.name, c.parent_id FROM categories c
        INNER JOIN category_tree ct ON c.parent_id = ct.id
    `)

// Subquery in WHERE clause
subQuery := db.Model(&Order{}).Select("user_id").Where("total > ?", 100)
db.Model(&User{}).WhereSubquery("id", "IN", subQuery)

// EXISTS subquery
orderSubQuery := db.Model(&Order{}).Where("orders.user_id = users.id")
db.Model(&User{}).WhereSubquery("", "EXISTS", orderSubQuery)

// Subquery in FROM clause
userStats := db.Model(&User{}).
    Select("id", "COUNT(*) as order_count").
    Group("id")
db.Model("").FromSubquery("user_stats", userStats)

// Window Functions - ROW_NUMBER, RANK, DENSE_RANK
db.Model(&Employee{}).
    Select("*").
    RowNumber("row_num").
    PartitionBy("department").
    WindowOrderBy("salary", false)

// Window Functions - Custom window function
db.Model(&Sales{}).
    Window("moving_avg", "AVG", "amount").
    PartitionBy("product_id").
    WindowOrderBy("sale_date", false).
    WindowFrame("ROWS", "2 PRECEDING", "CURRENT ROW", "")

// LAG/LEAD for accessing previous/next row values
db.Model(&Sales{}).
    Lag("prev_sales", "amount", 1).
    PartitionBy("product_id").
    WindowOrderBy("sale_date", false)

// HAVING clause for filtering aggregated results
db.Model(&Order{}).
    Select("user_id", "COUNT(*) as order_count").
    Group("user_id").
    Having("COUNT(*) > ?", 5)
```

### 🔒 Security Features

```go
import "github.com/gomodul/db/internal/security"

// Validate raw queries
warnings, err := security.ValidateRawQuery(sql, nil)
if err != nil {
    log.Fatalf("Unsafe query: %v", err)
}

// Validate field names from user input
if err := security.ValidateFieldName(userInput); err != nil {
    return fmt.Errorf("invalid field: %w", err)
}

// Sanitize field names
safeField := security.SanitizeFieldName(userInput)
```

### 🌊 Streaming Large Results

```go
// Cursor-based (memory efficient)
cursor, _ := db.Model(&User{}).Cursor()
defer cursor.Close()

for cursor.Next() {
    var user User
    cursor.Scan(&user)
    // Process user
}

// Callback function
ScanEach(
    db.Model(&User{}).Where("status = ?", "active"),
    func(user *User) error {
        fmt.Printf("User: %s\n", user.Name)
        return nil
    },
)

// Channel streaming
for item := range Stream(db.Model(&User{})) {
    if item.Error != nil {
        log.Printf("Error: %v", item.Error)
        continue
    }
    user := item.Data.(map[string]interface{})
}

// Batch processing
processor := NewBatchProcessor(1000)
processor.ProcessBatch(
    db.Model(&User{}),
    func(batch []*User) error {
        // Process batch
        return nil
    },
)
```

### Filtering Operations

| Operator | Method | Example |
|----------|--------|---------|
| Equal | `Where()` | `Where("id = ?", 1)` |
| Not Equal | `Where()` | `Where("status != ?", "deleted")` |
| Greater Than | `Where()` | `Where("age > ?", 18)` |
| IN | `In()` | `In("id", 1, 2, 3)` |
| NOT IN | `NotIn()` | `NotIn("id", 4, 5)` |
| BETWEEN | `Between()` | `Between("age", 18, 65)` |
| IS NULL | `Null()` | `Null("deleted_at")` |
| LIKE | `Like()` | `Like("name", "John%")` |
| AND | `Where()` | `Where("a = ? AND b = ?", 1, 2)` |
| OR | `Or()` | `Or("age < ?", 18)` |
| NOT | `Not()` | `Not("status = ?", "banned")` |

### 🔄 Transaction Support

```go
// Simple transaction with auto rollback
err := database.Transaction(func(tx *db.DB) error {
    if err := tx.Model(&User{}).Create(&user); err != nil {
        return err // Auto rollback
    }
    if err := tx.Model(&Profile{}).Create(&profile); err != nil {
        return err // Auto rollback
    }
    return nil // Auto commit
})
```

### Backend Switching

**Just change the DSN!**

```go
// PostgreSQL
db.Open(db.Config{DSN: "postgres://localhost:5432/mydb"})

// MySQL
db.Open(db.Config{DSN: "mysql://localhost:3306/mydb"})

// SQLite
db.Open(db.Config{DSN: "file:///tmp/mydb.sqlite"})

// MongoDB
db.Open(db.Config{DSN: "mongodb://localhost:27017"})

// Redis
db.Open(db.Config{DSN: "redis://localhost:6379"})

// Elasticsearch
db.Open(db.Config{DSN: "http://localhost:9200"})

// REST API
db.Open(db.Config{DSN: "https://api.example.com"})

// GraphQL
db.Open(db.Config{DSN: "https://api.example.com/graphql"})
```

### ⚡ Performance Optimization

```go
// Connection pool
database, err := db.Open(db.Config{
    DSN:            "postgres://user:pass@localhost:5432/mydb",
    MaxOpenConns:    25,
    MaxIdleConns:    5,
    ConnMaxLifetime: 30 * time.Minute,
    ConnMaxIdleTime: 5 * time.Minute,
})

// Retry for transient failures
database, err := db.Open(db.Config{
    RetryMaxRetries: 3,
    RetryBaseDelay:   100 * time.Millisecond,
    RetryMaxDelay:   1 * time.Second,
})

// Query caching
import "github.com/gomodul/db/cache"
database, err := db.Open(db.Config{
    Cache: cache.NewInMemoryCache(&cache.Config{
        MaxEntries: 1000,
        TTL:        5 * time.Minute,
    }),
})

// Connection pool monitoring
import "github.com/gomodul/db/pool"

database.EnablePoolMonitoring(&pool.Config{
    Name:            "main_db",
    CollectInterval: 30 * time.Second,
    AlertThresholds: pool.AlertThresholds{
        MaxOpenConnectionsUsage: 0.9,  // Alert at 90% capacity
        MaxWaitDuration:         5 * time.Second,
    },
})

// Get pool statistics
stats, _ := database.GetPoolStats(context.Background())
fmt.Printf("Active connections: %d/%d\n", stats.InUse, stats.MaxOpenConnections)

// Check pool health
health, _ := database.GetPoolHealth(context.Background())
if !health.Healthy {
    for _, warning := range health.Warnings {
        log.Printf("Pool warning: %s", warning)
    }
}

// Get pool information
info := database.GetPoolInfo()
fmt.Printf("Pool: %s, Open: %d, InUse: %d, Idle: %d\n",
    info.Name, info.CurrentOpen, info.InUse, info.Idle)
```

### 🔍 Error Handling

```go
import "github.com/gomodul/db"

// Detailed error with context
err := db.Errorf("Find", "users", "failed: %w", err).
    WithField("id", userID).
    WithQuery("SELECT * FROM users WHERE id = ?").
    WithDriver("postgres")

// Check error types
if db.IsErrNotFound(err) {
    // Handle not found
}
if db.IsRetryable(err) {
    // Retry logic
}
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Programmer's Repository Layer                  │
│              (Write ONCE, works EVERYWHERE)                  │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│           Universal Query Builder (Fluent API)              │
│  GORM-style: Where("id IN ?", []int{1,2,3}).Find()         │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│              Universal Query Model (IR)                     │
│              Backend-agnostic query representation           │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│              Query Translator Layer                         │
│  SQL │ Mongo │ Redis │ REST │ GraphQL │ gRPC │ Kafka         │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                   Driver Layer + Security                   │
│  SQL Injection Protection │ Prepared Statement Cache       │
└─────────────────────────────────────────────────────────────┘
```

## 🧪 Testing

```bash
# Run all tests
go test ./... -v

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## 📝 Examples

See the `examples/` directory for complete examples:

- [Basic Usage](examples/basic_usage.go) - Universal repository pattern
- [Backend Switching](examples/basic_usage.go) - Same code, different backends
- [Transactions](examples/basic_usage.go) - Transaction support
- [Complex Queries](examples/basic_usage.go) - Advanced filtering and sorting
- [REST API](examples/rest_api/main.go) - Production-ready REST API example
- [Best Practices Guide](BEST_PRACTICES.md) - Production-ready patterns and guidelines

## 📘 Best Practices

For production-ready applications, check out our [Best Practices Guide](BEST_PRACTICES.md) covering:

- **Project Structure** - Recommended directory layout
- **Repository Pattern** - Clean data access layer
- **Connection Management** - Pool configuration and monitoring
- **Transaction Management** - Proper transaction handling
- **Error Handling** - Wrapped errors with context
- **Security** - SQL injection prevention and validation
- **Performance** - Optimization techniques and streaming
- **Testing** - Unit and integration test patterns
- **Migration Strategy** - Versioned migrations
- **Monitoring** - Health checks and metrics

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by [GORM](https://gorm.io) - Fluent API design
- Built with love by the gomodul team

---

**⭐ Star us on GitHub!** - [github.com/gomodul/db](https://github.com/gomodul/db)
