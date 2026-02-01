package examples

import (
	"fmt"
	"log"
	"time"

	"github.com/gomodul/db"
	"github.com/gomodul/db/builder"
)

// User represents a user model
type User struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Status   string `json:"status"`
	Age      int    `json:"age"`
}

// UserRepository demonstrates universal repository pattern
// This code works with ALL backends without modification!
type UserRepository struct {
	db *builder.QueryBuilder
}

// NewUserRepository creates a new user repository
func NewUserRepository(database *db.DB) *UserRepository {
	return &UserRepository{
		db: database.Model(&User{}),
	}
}

// Example 1: Find by ID - Works with PostgreSQL, MySQL, SQLite, MongoDB, Redis, Elasticsearch, etc.
func (r *UserRepository) FindByID(id int64) (*User, error) {
	var user User
	err := r.db.Where("id = ?", id).First(&user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// Example 2: Find active users with pagination
func (r *UserRepository) FindActiveUsers(page, pageSize int) ([]*User, error) {
	var users []*User

	// Build query - universal across all backends
	err := r.db.
		Where("status = ?", "active").
		Order("created_at DESC").
		Limit(pageSize).
		Offset((page - 1) * pageSize).
		Find(&users)

	return users, err
}

// Example 3: Complex filtering with multiple conditions
func (r *UserRepository) SearchUsers(filter UserFilter) ([]*User, error) {
	var users []*User

	query := r.db

	// Add filters conditionally
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}

	if filter.MinAge > 0 {
		query = query.Where("age >= ?", filter.MinAge)
	}

	if filter.MaxAge > 0 {
		query = query.Where("age <= ?", filter.MaxAge)
	}

	if filter.Search != "" {
		query = query.Where("name LIKE ?", "%"+filter.Search+"%")
	}

	// Add sorting
	if filter.SortBy != "" {
		direction := "ASC"
		if filter.SortDesc {
			direction = "DESC"
		}
		query = query.Order(filter.SortBy + " " + direction)
	}

	err := query.Find(&users)
	return users, err
}

// UserFilter represents search criteria
type UserFilter struct {
	Status   string
	MinAge   int
	MaxAge   int
	Search   string
	SortBy   string
	SortDesc bool
}

// Example 4: Create user
func (r *UserRepository) Create(user *User) error {
	return r.db.Create(user)
}

// Example 5: Update user
func (r *UserRepository) Update(id int64, updates map[string]interface{}) error {
	return r.db.
		Where("id = ?", id).
		Updates(updates)
}

// Example 6: Delete user
func (r *UserRepository) Delete(id int64) error {
	return r.db.
		Where("id = ?", id).
		Delete()
}

// Example 7: Count users by status
func (r *UserRepository) CountByStatus(status string) (int64, error) {
	// For SQL drivers, use Exec to get count
	rowsAffected, err := r.db.
		Where("status = ?", status).
		Exec("SELECT COUNT(*) AS count FROM users WHERE status = $1", status)
	return rowsAffected, err
}

// Example 8: Batch operations
func (r *UserRepository) CreateBatch(users []*User) error {
	for _, user := range users {
		if err := r.db.Create(user); err != nil {
			return err
		}
	}
	return nil
}

// ============================================
// SWITCHING BACKENDS - Only change DSN!
// ============================================

func ExampleBackendSwitching() {
	// PostgreSQL
	pgDB, _ := db.Open(db.Config{
		DSN: "postgres://user:pass@localhost:5432/mydb",
	})
	pgRepo := NewUserRepository(pgDB)

	// MySQL
	mysqlDB, _ := db.Open(db.Config{
		DSN: "mysql://user:pass@localhost:3306/mydb",
	})
	mysqlRepo := NewUserRepository(mysqlDB)

	// SQLite
	sqliteDB, _ := db.Open(db.Config{
		DSN: "file:///tmp/mydb.sqlite",
	})
	sqliteRepo := NewUserRepository(sqliteDB)

	// MongoDB
	mongoDB, _ := db.Open(db.Config{
		DSN: "mongodb://localhost:27017",
	})
	mongoRepo := NewUserRepository(mongoDB)

	// Redis
	redisDB, _ := db.Open(db.Config{
		DSN: "redis://localhost:6379",
	})
	redisRepo := NewUserRepository(redisDB)

	// Elasticsearch
	esDB, _ := db.Open(db.Config{
		DSN: "http://localhost:9200",
	})
	esRepo := NewUserRepository(esDB)

	// REST API
	restDB, _ := db.Open(db.Config{
		DSN: "https://api.example.com",
	})
	restRepo := NewUserRepository(restDB)

	// GraphQL API
	gqlDB, _ := db.Open(db.Config{
		DSN: "https://api.example.com/graphql",
	})
	gqlRepo := NewUserRepository(gqlDB)

	// All repositories work identically!
	_ = pgRepo
	_ = mysqlRepo
	_ = sqliteRepo
	_ = mongoRepo
	_ = redisRepo
	_ = esRepo
	_ = restRepo
	_ = gqlRepo
}

// ============================================
// USAGE EXAMPLES
// ============================================

func main() {
	// Initialize database (change DSN to switch backends!)
	database, err := db.Open(db.Config{
		DSN: "postgres://user:pass@localhost:5432/mydb",
		// DSN: "mongodb://localhost:27017"  // Just change DSN!
		// DSN: "https://api.example.com"  // Just change DSN!
	})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	repo := NewUserRepository(database)

	// Create a user
	user := &User{
		Name:   "John Doe",
		Email:  "john@example.com",
		Status: "active",
		Age:    30,
	}
	if err := repo.Create(user); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created user with ID: %v\n", user.ID)

	// Find by ID
	found, err := repo.FindByID(user.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found user: %+v\n", found)

	// Search users
	users, err := repo.SearchUsers(UserFilter{
		Status: "active",
		MinAge: 25,
		MaxAge: 40,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d users\n", len(users))

	// Update user
	err = repo.Update(user.ID, map[string]interface{}{
		"status": "inactive",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Delete user
	err = repo.Delete(user.ID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("User deleted")
}

// ============================================
// RETRY & ROLLBACK EXAMPLES
// ============================================

// ExampleRetryConfiguration demonstrates automatic retry on transient failures
func ExampleRetryConfiguration() error {
	// Configure retry for transient failures (network errors, timeouts, etc.)
	database, err := db.Open(db.Config{
		DSN: "postgres://user:pass@localhost:5432/mydb",
		// Retry settings
		RetryMaxRetries: 3,                    // Max retry attempts
		RetryBaseDelay:  100 * time.Millisecond, // Initial delay
		RetryMaxDelay:   1 * time.Second,       // Max delay
	})
	if err != nil {
		return err
	}
	defer database.Close()

	// Queries automatically retry on transient failures
	// No need to manually handle retry logic!
	repo := NewUserRepository(database)
	user, err := repo.FindByID(1)
	if err != nil {
		return err
	}

	fmt.Printf("Found user: %+v\n", user)
	return nil
}

// Note: Transactions automatically rollback on error or panic
// See ExampleTransactions below for transaction handling

// ============================================
// TRANSACTION EXAMPLES
// ============================================

func ExampleTransactions(database *db.DB) error {
	// Use the Transaction function
	return database.Transaction(func(txDB *db.DB) error {
		repo := NewUserRepository(txDB)

		user1 := &User{Name: "User 1", Email: "user1@example.com"}
		user2 := &User{Name: "User 2", Email: "user2@example.com"}

		if err := repo.Create(user1); err != nil {
			return err // Will automatically rollback
		}

		if err := repo.Create(user2); err != nil {
			return err // Will automatically rollback
		}

		fmt.Println("Transaction completed successfully")
		return nil // Will automatically commit
	})
}

// ============================================
// TRANSACTION WITH QUERYBUILDER
// ============================================

func ExampleTransactionWithQueryBuilder(qb *builder.QueryBuilder) error {
	// Begin transaction using QueryBuilder
	tx, err := qb.BeginTx()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Use transaction's QueryBuilder
	user1 := &User{Name: "User 1", Email: "user1@example.com"}
	user2 := &User{Name: "User 2", Email: "user2@example.com"}

	// Execute operations within transaction using tx.Tx() to get the QueryBuilder
	txQB := tx.Tx()
	txRepo := &UserRepository{db: txQB}
	if err = txRepo.Create(user1); err == nil {
		err = txRepo.Create(user2)
	}

	if err != nil {
		return err
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return err
	}

	fmt.Println("Transaction completed successfully")
	return nil
}
