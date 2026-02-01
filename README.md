# gomodul/db - Universal Database Abstraction Layer for Go

<div align="center">

**Write your repository code ONCE, use it with ANY backend!**

[![Go Report Card](https://goreportcard.com/badge/github.com/gomodul/db)](https://goreportcard.com/report/github.com/gomodul/db)
[![GoDoc](https://godoc.org/github.com/gomodul/db?status.svg)](https://godoc.org/github.com/gomodul/db)

</div>

## 🎯 Goal

`gomodul/db` allows you to write your repository code **once** and seamlessly switch between **11 different backends** by only changing the connection DSN - no code modifications needed!

### Supported Backends

| Type | Driver | Status |
|------|--------|--------|
| **SQL** | PostgreSQL, MySQL, SQLite | ✅ Full Support |
| **NoSQL** | MongoDB, Redis | ✅ Full Support |
| **Search** | Elasticsearch | ✅ Full Support |
| **API** | REST, GraphQL | ✅ Full Support |
| **RPC** | gRPC | ✅ Full Support |
| **Message Queue** | Kafka | ✅ Full Support |

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
    "github.com/gomodul/db/builder"
)

type User struct {
    ID    int64  `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

func main() {
    // Open database connection
    database, err := db.Open(db.Config{
        DSN: "postgres://user:pass@localhost:5432/mydb",
        // Change DSN to switch backends!
        // DSN: "mongodb://localhost:27017/mydb"
        // DSN: "redis://localhost:6379"
        // DSN: "https://api.example.com"
    })
    if err != nil {
        log.Fatal(err)
    }
    defer database.Close()

    // Create repository
    repo := NewUserRepository(database)

    // Use it!
    user, err := repo.FindByID(1)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Found user: %+v", user)
}

type UserRepository struct {
    db *builder.QueryBuilder
}

func NewUserRepository(database *db.DB) *UserRepository {
    return &UserRepository{db: database.Model(&User{})}
}

func (r *UserRepository) FindByID(id int64) (*User, error) {
    var user User
    err := r.db.Where("id = ?", id).First(&user)
    return &user, err
}
```

## 📖 Documentation

### Auto-Retry on Transient Failures

The library automatically retries failed queries on transient errors (network issues, timeouts, connection resets). Non-transient errors (like "not found" or "duplicate key") are not retried.

```go
database, err := db.Open(db.Config{
    DSN: "postgres://user:pass@localhost:5432/mydb",
    // Retry configuration
    RetryMaxRetries: 3,                    // Max retry attempts
    RetryBaseDelay:  100 * time.Millisecond, // Initial delay
    RetryMaxDelay:   1 * time.Second,       // Max delay between retries
})

// Queries automatically retry on transient failures
db.Model(&User{}).Where("id = ?", 1).First(&user)
```

**Transient errors that trigger retry:**
- Connection refused/reset
- Broken pipe
- Timeout/deadline exceeded
- Network unreachable
- Temporary failures

**Non-transient errors (no retry):**
- Record not found
- Duplicate key
- Validation errors
- Transaction done

### Universal Query API

```go
// Finding records
db.Model(&User{}).Where("status = ?", "active").Find(&users)
db.Model(&User{}).Where("age >= ?", 18).Limit(10).Find(&users)
db.Model(&User{}).Order("created_at DESC").Offset(10).Limit(10).Find(&users)

// Creating records
db.Model(&User{}).Create(&User{Name: "John", Email: "john@example.com"})

// Updating records
db.Model(&User{}).Where("id = ?", 1).Update(map[string]interface{}{
    "status": "inactive",
})

// Deleting records
db.Model(&User{}).Where("id = ?", 1).Delete()

// Counting
db.Model(&User{}).Where("status = ?", "active").Count()
```

### Filtering Operations

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `Where("id = ?", 1)` |
| `!=` | Not Equal | `Where("status != ?", "deleted")` |
| `>`, `>=`, `<`, `<=` | Comparison | `Where("age > ?", 18)` |
| `IN` | In List | `Where("id IN ?", []int{1,2,3})` |
| `NOT IN` | Not In List | `Where("id NOT IN ?", []int{1,2})` |
| `LIKE` | Pattern Match | `Where("name LIKE ?", "%John%")` |
| `BETWEEN` | Range | `Where("age BETWEEN ?", []int{18,65})` |
| `IS NULL` | Null Check | `Where("deleted_at IS NULL")` |
| `AND`, `OR` | Logical | `Where("status = ? AND age >= ?", "active", 18)` |

### Transaction Support

```go
tx, err := database.BeginTx(ctx)
if err != nil {
    return err
}

defer func() {
    if err != nil {
        tx.Rollback()
    } else {
        tx.Commit()
    }
}()

// Execute operations within transaction
repo := NewUserRepository(database)
err = repo.Create(user1)
if err == nil {
    err = repo.Create(user2)
}
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
db.Open(db.Config{
    DSN: "mongodb://localhost:27017",
    Database: "mydb",
})

// Redis
db.Open(db.Config{DSN: "redis://localhost:6379"})

// Elasticsearch
db.Open(db.Config{DSN: "http://localhost:9200"})

// REST API
db.Open(db.Config{
    DSN: "https://api.example.com",
    Options: map[string]interface{}{
        "token": "your-api-token",
    },
})

// GraphQL
db.Open(db.Config{
    DSN: "https://api.example.com/graphql",
    Options: map[string]interface{}{
        "token": "your-graphql-token",
    },
})

// gRPC
db.Open(db.Config{DSN: "localhost:50051"})

// Kafka
db.Open(db.Config{DSN: "localhost:9092"})
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
│  db.Model(&User{}).Where("status = ?", "active").Find()    │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│              Universal Query Model (IR)                     │
│              Backend-agnostic query representation           │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│              Query Translator Layer                         │
│  SQL Translator │ Mongo Translator │ REST Translator        │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                   Driver Layer                              │
│  SQL │ NoSQL │ API │ RPC │ GraphQL │ Message Queue           │
└─────────────────────────────────────────────────────────────┘
```

## 🧪 Testing

```bash
go test ./... -v
```

## 📝 Examples

See the `examples/` directory for complete examples:

- [Basic Usage](examples/basic_usage.go) - Universal repository pattern
- [Backend Switching](examples/basic_usage.go) - Same code, different backends
- [Transactions](examples/basic_usage.go) - Transaction support
- [Complex Queries](examples/basic_usage.go) - Advanced filtering and sorting

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Inspired by [GORM](https://gorm.io)
- Built with [go-redis](https://github.com/redis/go-redis)
- Built with [mongo-driver](https://github.com/mongodb/mongo-go-driver)
- Built with [go-elasticsearch](https://github.com/elastic/go-elasticsearch)
