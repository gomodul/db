# gomodul/db - Best Practices Guide

This guide covers production-ready patterns and best practices for using `gomodul/db` in real-world applications.

## Table of Contents

1. [Project Structure](#project-structure)
2. [Repository Pattern](#repository-pattern)
3. [Connection Management](#connection-management)
4. [Transaction Management](#transaction-management)
5. [Error Handling](#error-handling)
6. [Security Best Practices](#security-best-practices)
7. [Performance Optimization](#performance-optimization)
8. [Testing](#testing)
9. [Migration Strategy](#migration-strategy)
10. [Monitoring and Observability](#monitoring-and-observability)

---

## Project Structure

### Recommended Directory Layout

```
project/
├── cmd/
│   └── api/
│       └── main.go
├── internal/
│   ├── model/          # Domain models
│   │   ├── user.go
│   │   └── post.go
│   ├── repository/     # Data access layer
│   │   ├── user.go
│   │   ├── post.go
│   │   └── repository.go
│   ├── service/        # Business logic layer
│   │   ├── user.go
│   │   └── post.go
│   └── handler/        # HTTP handlers
│       ├── user.go
│       └── post.go
├── pkg/
│   └── database/       # Database initialization
│       └── database.go
├── migrations/         # SQL migration files
├── config/
│   └── config.go
└── go.mod
```

---

## Repository Pattern

### Why Use Repository Pattern?

The repository pattern provides:
- **Separation of concerns**: Isolates data access logic
- **Testability**: Easy to mock for testing
- **Maintainability**: Centralizes database queries
- **Flexibility**: Easy to swap implementations

### Example Repository

```go
// internal/repository/user.go
package repository

import (
    "context"
    "github.com/gomodul/db"
    "yourapp/internal/model"
)

type UserRepository struct {
    db *db.DB
}

func NewUserRepository(database *db.DB) *UserRepository {
    return &UserRepository{db: database}
}

// Create creates a new user
func (r *UserRepository) Create(ctx context.Context, user *model.User) error {
    return r.db.Model(&model.User{}).
        WithContext(ctx).
        Create(user)
}

// FindByID retrieves a user by ID
func (r *UserRepository) FindByID(ctx context.Context, id int64) (*model.User, error) {
    var user model.User
    err := r.db.Model(&model.User{}).
        WithContext(ctx).
        Where("id = ?", id).
        First(&user)

    if err != nil {
        return nil, err
    }
    return &user, nil
}

// List retrieves users with filters and pagination
func (r *UserRepository) List(ctx context.Context, opts *ListOptions) ([]*model.User, int64, error) {
    // Build query
    query := r.db.Model(&model.User{}).WithContext(ctx)

    // Apply filters
    if opts.Status != "" {
        query = query.Where("status = ?", opts.Status)
    }
    if opts.MinAge > 0 {
        query = query.Where("age >= ?", opts.MinAge)
    }

    // Count total
    total, err := query.Count()
    if err != nil {
        return nil, 0, err
    }

    // Apply pagination
    if opts.Page > 0 && opts.PageSize > 0 {
        query = query.Paginate(opts.Page, opts.PageSize)
    }

    // Execute query
    var users []*model.User
    if err := query.Find(&users); err != nil {
        return nil, 0, err
    }

    return users, total, nil
}

// Update updates a user
func (r *UserRepository) Update(ctx context.Context, user *model.User) error {
    updates := map[string]interface{}{
        "name":       user.Name,
        "email":      user.Email,
        "updated_at": time.Now(),
    }

    return r.db.Model(&model.User{}).
        WithContext(ctx).
        Where("id = ?", user.ID).
        Update(updates)
}

// Delete soft deletes a user
func (r *UserRepository) Delete(ctx context.Context, id int64) error {
    return r.db.Model(&model.User{}).
        WithContext(ctx).
        Where("id = ?", id).
        Update(map[string]interface{}{
            "deleted_at": time.Now(),
        })
}
```

---

## Connection Management

### Single Database Instance

```go
// pkg/database/database.go
package database

import (
    "time"
    "github.com/gomodul/db"
)

var DB *db.DB

// Init initializes the database connection
func Init(dsn string) error {
    var err error
    DB, err = db.Open(db.Config{
        DSN:            dsn,
        MaxOpenConns:   25,
        MaxIdleConns:   5,
        ConnMaxLifetime: 30 * time.Minute,
        ConnMaxIdleTime: 5 * time.Minute,

        // Retry configuration
        RetryMaxRetries: 3,
        RetryBaseDelay:  100 * time.Millisecond,
        RetryMaxDelay:   1 * time.Second,
    })

    if err != nil {
        return err
    }

    // Enable monitoring
    DB.EnablePoolMonitoring(nil)

    return nil
}

// Close closes the database connection
func Close() error {
    if DB != nil {
        return DB.Close()
    }
    return nil
}
```

### Environment-Specific Configuration

```go
// config/config.go
package config

import (
    "os"
    "time"
)

type Config struct {
    DatabaseDSN string
}

func Load() *Config {
    return &Config{
        DatabaseDSN: os.Getenv("DATABASE_DSN"),
    }
}
```

---

## Transaction Management

### Simple Transactions

```go
func (s *UserService) CreateUserWithProfile(ctx context.Context, req *CreateUserRequest) error {
    return s.db.Transaction(func(tx *db.DB) error {
        // Create user
        user := &model.User{
            Name:  req.Name,
            Email: req.Email,
        }
        if err := tx.Model(&model.User{}).Create(user); err != nil {
            return err // Auto rollback
        }

        // Create profile
        profile := &model.Profile{
            UserID: user.ID,
            Bio:    req.Bio,
        }
        if err := tx.Model(&model.Profile{}).Create(profile); err != nil {
            return err // Auto rollback
        }

        return nil // Auto commit
    })
}
```

### Nested Transactions with Savepoints

```go
func (s *OrderService) ProcessOrder(ctx context.Context, orderID int64) error {
    return s.db.Transaction(func(tx *db.DB) error {
        // Load order
        order, err := s.getOrder(tx, orderID)
        if err != nil {
            return err
        }

        // Update inventory
        for _, item := range order.Items {
            if err := s.updateInventory(tx, item); err != nil {
                return err // Rollback entire transaction
            }
        }

        // Update order status
        if err := s.updateOrderStatus(tx, orderID, "processed"); err != nil {
            return err
        }

        return nil
    })
}
```

---

## Error Handling

### Wrapped Errors with Context

```go
func (r *UserRepository) FindByID(ctx context.Context, id int64) (*model.User, error) {
    var user model.User
    err := r.db.Model(&model.User{}).
        WithContext(ctx).
        Where("id = ?", id).
        First(&user)

    if err != nil {
        // Wrap error with context
        return nil, db.Errorf("Find", "users", "failed to find user by ID %d: %w", id, err).
            WithField("user_id", id).
            WithQuery("SELECT * FROM users WHERE id = ?").
            WithDriver("postgres")
    }

    return &user, nil
}

// Checking error types
func (s *UserService) GetUser(ctx context.Context, id int64) (*model.User, error) {
    user, err := s.repo.FindByID(ctx, id)
    if err != nil {
        if db.IsErrNotFound(err) {
            return nil, fmt.Errorf("user not found: %d", id)
        }
        if db.IsRetryable(err) {
            // Retry logic
            return s.retryGetUser(ctx, id)
        }
        return nil, err
    }
    return user, nil
}
```

---

## Security Best Practices

### 1. Always Use Parameterized Queries

```go
// ✅ GOOD - Parameterized
db.Model(&User{}).Where("email = ?", userInput).First(&user)

// ❌ BAD - SQL injection vulnerable
db.Exec(fmt.Sprintf("SELECT * FROM users WHERE email = '%s'", userInput))
```

### 2. Validate Field Names

```go
import "github.com/gomodul/db/internal/security"

func validateFieldName(field string) error {
    if err := security.ValidateFieldName(field); err != nil {
        return fmt.Errorf("invalid field name: %w", err)
    }
    return nil
}

// Usage
userInput := r.URL.Query().Get("sort_by")
if err := validateFieldName(userInput); err != nil {
    return err
}
db.Model(&User{}).Order(userInput + " DESC").Find(&users)
```

### 3. Sanitize Raw Queries

```go
import "github.com/gomodul/db/internal/security"

func executeRawQuery(query string) error {
    warnings, err := security.ValidateRawQuery(query, nil)
    if err != nil {
        return fmt.Errorf("unsafe query: %w", err)
    }

    if len(warnings) > 0 {
        // Log warnings but proceed
        for _, w := range warnings {
            log.Printf("Security warning: %s", w.Message)
        }
    }

    return db.Exec(query)
}
```

### 4. Use AutoMigrate for Schema

```go
// In production, prefer explicit migrations over AutoMigrate
// Use AutoMigrate only for development or simple schemas

if err := database.AutoMigrate(&User{}, &Post{}); err != nil {
    log.Fatalf("Migration failed: %v", err)
}
```

---

## Performance Optimization

### 1. Use Prepared Statement Caching

```go
// Prepared statements are cached automatically
// Reuse the same query for better performance

for _, id := range userIDs {
    var user User
    // This uses cached prepared statement
    db.Model(&User{}).Where("id = ?", id).First(&user)
}
```

### 2. Batch Operations

```go
// ✅ GOOD - Batch insert
users := []*User{user1, user2, user3}
for _, user := range users {
    db.Model(&User{}).Create(user)
}

// ✅ GOOD - Batch update
db.Model(&User{}).
    Where("status = ?", "inactive").
    Update(map[string]interface{}{
        "deleted_at": time.Now(),
    })
```

### 3. Use Streaming for Large Results

```go
// ✅ GOOD - Streaming for large datasets
cursor, _ := db.Model(&User{}).Where("status = ?", "active").Cursor()
defer cursor.Close()

for cursor.Next() {
    var user User
    if err := cursor.Scan(&user); err != nil {
        return err
    }
    // Process user
}

// ✅ GOOD - ScanEach for processing
db.ScanEach(
    db.Model(&User{}).Where("status = ?", "active"),
    func(user *User) error {
        // Process user
        return nil
    },
)

// ❌ BAD - Loads all into memory
var users []User
db.Model(&User{}).Where("status = ?", "active").Find(&users)
for _, user := range users {
    // Process user
}
```

### 4. Optimize Connection Pool

```go
database, err := db.Open(db.Config{
    DSN:            dsn,
    MaxOpenConns:   25,  // Maximum open connections
    MaxIdleConns:   5,   // Maximum idle connections
    ConnMaxLifetime: 30 * time.Minute,
    ConnMaxIdleTime: 5 * time.Minute,
})
```

### 5. Use Query Caching

```go
import "github.com/gomodul/db/cache"

database, err := db.Open(db.Config{
    DSN: dsn,
    Cache: cache.NewInMemoryCache(&cache.Config{
        MaxEntries: 1000,
        TTL:        5 * time.Minute,
    }),
})
```

### 6. Select Only Needed Fields

```go
// ✅ GOOD - Select only needed fields
db.Model(&User{}).
    Select("id", "name", "email").
    Find(&users)

// ❌ BAD - Selects all fields
db.Model(&User{}).Find(&users)
```

### 7. Use Indexes

```go
// In your model tags
type User struct {
    ID    int64  `db:"id,pk"`
    Email string `db:"email,unique,index"`
    Age   int    `db:"age,index"`
    Status string `db:"status,index"`
}

// Or create indexes manually
migrator := database.Migrator()
migrator.CreateIndex(ctx, &User{}, &migrate.IndexInfo{
    Name:    "idx_user_email_status",
    Columns: []string{"email", "status"},
    Unique:  false,
})
```

---

## Testing

### Unit Tests with Mocks

```go
// internal/service/user_test.go
package service_test

import (
    "context"
    "testing"

    "github.com/gomodul/db"
    "yourapp/internal/model"
)

// Mock repository
type mockUserRepository struct {
    users map[int64]*model.User
}

func (m *mockUserRepository) FindByID(ctx context.Context, id int64) (*model.User, error) {
    user, ok := m.users[id]
    if !ok {
        return nil, db.ErrNotFound
    }
    return user, nil
}

func TestUserService_GetUser(t *testing.T) {
    // Setup
    mockRepo := &mockUserRepository{
        users: map[int64]*model.User{
            1: {ID: 1, Name: "Test User"},
        },
    }
    service := NewUserService(mockRepo)

    // Test
    user, err := service.GetUser(context.Background(), 1)

    // Assert
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if user.Name != "Test User" {
        t.Errorf("expected name 'Test User', got '%s'", user.Name)
    }
}
```

### Integration Tests

```go
// internal/repository/user_integration_test.go
// +build integration

package repository_test

import (
    "context"
    "testing"
    "github.com/gomodul/db"
)

func TestUserRepository_Integration(t *testing.T) {
    // Setup test database
    database, err := db.Open(db.Config{
        DSN: "postgres://test:test@localhost:5432/testdb",
    })
    if err != nil {
        t.Skip("Database not available")
    }
    defer database.Close()

    // Auto-migrate
    if err := database.AutoMigrate(&model.User{}); err != nil {
        t.Fatalf("migration failed: %v", err)
    }

    repo := repository.NewUserRepository(database)

    // Test Create
    user := &model.User{Name: "Test", Email: "test@example.com"}
    if err := repo.Create(context.Background(), user); err != nil {
        t.Errorf("Create failed: %v", err)
    }

    // Test FindByID
    found, err := repo.FindByID(context.Background(), user.ID)
    if err != nil {
        t.Errorf("FindByID failed: %v", err)
    }
    if found.Name != user.Name {
        t.Errorf("expected name %s, got %s", user.Name, found.Name)
    }
}
```

---

## Migration Strategy

### Versioned Migrations

```go
// migrations/migrations.go
package migrations

import (
    "context"
    "github.com/gomodul/db"
    "github.com/gomodul/db/migrate"
)

type Migration struct {
    Version int
    Name    string
    Up      func(*migrate.Migrator, context.Context) error
    Down    func(*migrate.Migrator, context.Context) error
}

var AllMigrations = []Migration{
    {
        Version: 1,
        Name:    "create_users_table",
        Up: func(m *migrate.Migrator, ctx context.Context) error {
            type User struct {
                ID        int64  `db:"id,pk,autoIncrement"`
                Name      string `db:"name,size:100"`
                Email     string `db:"email,unique"`
                CreatedAt string `db:"created_at"`
            }
            return m.AutoMigrate(ctx, &User{})
        },
        Down: func(m *migrate.Migrator, ctx context.Context) error {
            return m.DropTable(ctx, &User{})
        },
    },
    {
        Version: 2,
        Name:    "add_users_age_column",
        Up: func(m *migrate.Migrator, ctx context.Context) error {
            // Add column manually
            query := "ALTER TABLE users ADD COLUMN age INT"
            return m.db.Exec(ctx, query)
        },
        Down: func(m *migrate.Migrator, ctx context.Context) error {
            query := "ALTER TABLE users DROP COLUMN age"
            return m.db.Exec(ctx, query)
        },
    },
}
```

---

## Monitoring and Observability

### Health Checks

```go
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()

    // Check database health
    health, err := s.db.GetPoolHealth(ctx)
    if err != nil {
        http.Error(w, "Database unhealthy", http.StatusServiceUnavailable)
        return
    }

    if !health.Healthy {
        // Log warnings
        for _, warning := range health.Warnings {
            log.Printf("Pool warning: %s", warning)
        }
    }

    // Return health status
    json.NewEncoder(w).Encode(map[string]interface{}{
        "status":     "healthy",
        "pool_stats": health,
    })
}
```

### Metrics Collection

```go
import "github.com/gomodul/db/metrics"

// Create custom metrics collector
collector := metrics.NewDefaultCollector()

database, err := db.Open(db.Config{
    DSN: dsn,
    Metrics: collector,
})

// Collect metrics periodically
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        if err := database.CollectPoolMetrics(context.Background()); err != nil {
            log.Printf("Failed to collect metrics: %v", err)
        }

        // Get all metrics
        stats := collector.GetStats()
        log.Printf("Metrics: %+v", stats)
    }
}()
```

---

## Common Pitfalls

### 1. Forgetting Context

```go
// ❌ BAD - No context timeout
db.Model(&User{}).Where("id = ?", id).First(&user)

// ✅ GOOD - With context and timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
db.Model(&User{}).WithContext(ctx).Where("id = ?", id).First(&user)
```

### 2. Not Checking Errors

```go
// ❌ BAD - Ignoring errors
db.Model(&User{}).Create(user)

// ✅ GOOD - Always check errors
if err := db.Model(&User{}).Create(user); err != nil {
    return fmt.Errorf("failed to create user: %w", err)
}
```

### 3. N+1 Query Problem

```go
// ❌ BAD - N+1 queries
var posts []Post
db.Model(&Post{}).Find(&posts)
for _, post := range posts {
    var user User
    db.Model(&User{}).Where("id = ?", post.AuthorID).First(&user)
    post.Author = &user
}

// ✅ GOOD - Single query with preload
db.Model(&Post{}).
    Preload("Author").
    Find(&posts)

// ✅ GOOD - Use JOIN
db.Model(&Post{}).
    Join("users", "users.id = posts.author_id").
    Find(&posts)
```

### 4. Memory Leaks with Large Results

```go
// ❌ BAD - Loads all into memory
var users []User
db.Model(&User{}).Find(&users)

// ✅ GOOD - Streaming
cursor, _ := db.Model(&User{}).Cursor()
defer cursor.Close()
for cursor.Next() {
    var user User
    cursor.Scan(&user)
    // Process user immediately
}
```

---

## Conclusion

Following these best practices will help you build production-ready applications with `gomodul/db` that are:

- **Maintainable**: Clean separation of concerns
- **Performant**: Optimized queries and connection management
- **Secure**: Protected against common vulnerabilities
- **Reliable**: Proper error handling and transactions
- **Observable**: Monitored and measurable

For more examples, see the `examples/` directory in the repository.
