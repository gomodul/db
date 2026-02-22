package db

import (
	"context"
	"testing"
	"time"

	"github.com/gomodul/db/builder"
	"github.com/gomodul/db/internal/security"
)

// TestDB_Open tests opening a database connection
func TestDB_Open(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{
			name:    "Invalid DSN",
			dsn:     "invalid://dsn",
			wantErr: true,
		},
		{
			name:    "Empty DSN",
			dsn:     "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(Config{DSN: tt.dsn})
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if db != nil {
				db.Close()
			}
		})
	}
}

// TestDB_Config tests database configuration
func TestDB_Config(t *testing.T) {
	cfg := Config{
		DSN:            "test://localhost",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		RetryMaxRetries: 3,
		RetryBaseDelay:  100 * time.Millisecond,
		RetryMaxDelay:   time.Second,
	}

	if cfg.MaxOpenConns != 10 {
		t.Errorf("MaxOpenConns = %v, want 10", cfg.MaxOpenConns)
	}
	if cfg.RetryMaxRetries != 3 {
		t.Errorf("RetryMaxRetries = %v, want 3", cfg.RetryMaxRetries)
	}
}

// TestDB_Session tests creating database sessions
func TestDB_Session(t *testing.T) {
	db := &DB{
		Config: &Config{},
	}

	session := db.Session()
	if session == nil {
		t.Error("Session() returned nil")
	}
	if session.clone != 1 {
		t.Errorf("Session() clone = %v, want 1", session.clone)
	}
}

// TestDB_Model tests starting a query on a model
func TestDB_Model(t *testing.T) {
	db := &DB{
		Config: &Config{},
	}

	type User struct {
		ID   int64
		Name string
	}

	qb := db.Model(&User{})
	if qb == nil {
		t.Fatal("Model() returned nil")
	}

	collection := qb.GetCollection()
	if collection == "" {
		t.Error("Model() collection is empty")
	}
}

// TestDB_Table tests starting a query on a table
func TestDB_Table(t *testing.T) {
	db := &DB{
		Config: &Config{},
	}

	qb := db.Table("users")
	if qb == nil {
		t.Fatal("Table() returned nil")
	}

	collection := qb.GetCollection()
	if collection != "users" {
		t.Errorf("Table() collection = %v, want users", collection)
	}
}

// TestDB_Ping tests database ping
func TestDB_Ping(t *testing.T) {
	db := &DB{
		Config: &Config{},
	}

	ctx := context.Background()
	err := db.Ping(ctx)
	if err == nil {
		t.Error("Ping() should return error for nil driver")
	}
}

// TestDB_Health tests database health check
func TestDB_Health(t *testing.T) {
	db := &DB{
		Config: &Config{},
	}

	status, err := db.Health()
	if err != nil {
		t.Errorf("Health() unexpected error = %v", err)
	}
	if status == nil {
		t.Error("Health() returned nil status")
	}
}

// TestDB_Transaction tests transaction execution
func TestDB_Transaction(t *testing.T) {
	db := &DB{
		Config: &Config{},
	}

	err := db.Transaction(func(tx *DB) error {
		return nil
	})
	if err == nil {
		t.Error("Transaction() should return error for nil driver")
	}
}

// TestDB_Close tests closing the database connection
func TestDB_Close(t *testing.T) {
	db := &DB{
		Config: &Config{},
	}

	err := db.Close()
	if err != nil {
		t.Errorf("Close() unexpected error = %v", err)
	}
}

// TestBuilder_Where tests the Where method with various filters
func TestBuilder_Where(t *testing.T) {
	type User struct {
		ID   int64
		Name string
		Age  int
	}

	tests := []struct {
		name   string
		filter interface{}
		args   []interface{}
	}{
		{
			name:   "String filter with args",
			filter: "age > ?",
			args:   []interface{}{18},
		},
		{
			name:   "Map filter",
			filter: map[string]interface{}{"status": "active"},
			args:   nil,
		},
		{
			name:   "Struct filter",
			filter: User{Name: "John"},
			args:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{Config: &Config{}}
			qb := db.Model(&User{}).Where(tt.filter, tt.args...)
			if qb == nil {
				t.Error("Where() returned nil")
			}
		})
	}
}

// TestBuilder_In tests the In method
func TestBuilder_In(t *testing.T) {
	type User struct {
		ID   int64
		Name string
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).In("id", 1, 2, 3)
	if qb == nil {
		t.Error("In() returned nil")
	}
}

// TestBuilder_NotIn tests the NotIn method
func TestBuilder_NotIn(t *testing.T) {
	type User struct {
		ID   int64
		Name string
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).NotIn("id", 1, 2, 3)
	if qb == nil {
		t.Error("NotIn() returned nil")
	}
}

// TestBuilder_Between tests the Between method
func TestBuilder_Between(t *testing.T) {
	type User struct {
		ID   int64
		Name string
		Age  int
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Between("age", 18, 65)
	if qb == nil {
		t.Error("Between() returned nil")
	}
}

// TestBuilder_Null tests the Null method
func TestBuilder_Null(t *testing.T) {
	type User struct {
		ID        int64
		DeletedAt *time.Time
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Null("deleted_at")
	if qb == nil {
		t.Error("Null() returned nil")
	}
}

// TestBuilder_Like tests the Like method
func TestBuilder_Like(t *testing.T) {
	type User struct {
		ID   int64
		Name string
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Like("name", "John%")
	if qb == nil {
		t.Error("Like() returned nil")
	}
}

// TestBuilder_Order tests the Order method
func TestBuilder_Order(t *testing.T) {
	type User struct {
		ID   int64
		Name string
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Order("created_at DESC")
	if qb == nil {
		t.Error("Order() returned nil")
	}
}

// TestBuilder_Limit tests the Limit method
func TestBuilder_Limit(t *testing.T) {
	type User struct {
		ID   int64
		Name string
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Limit(10)
	if qb == nil {
		t.Error("Limit() returned nil")
	}

	if got := qb.GetLimit(); got != 10 {
		t.Errorf("Limit() = %v, want 10", got)
	}
}

// TestBuilder_Offset tests the Offset method
func TestBuilder_Offset(t *testing.T) {
	type User struct {
		ID   int64
		Name string
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Offset(20)
	if qb == nil {
		t.Error("Offset() returned nil")
	}

	if got := qb.GetOffset(); got != 20 {
		t.Errorf("Offset() = %v, want 20", got)
	}
}

// TestBuilder_Select tests the Select method
func TestBuilder_Select(t *testing.T) {
	type User struct {
		ID   int64
		Name string
		Age  int
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Select("id", "name")
	if qb == nil {
		t.Error("Select() returned nil")
	}
}

// TestBuilder_Join tests the Join methods
func TestBuilder_Join(t *testing.T) {
	type User struct {
		ID       int64
		ProfileID int64
	}
	type Profile struct {
		ID   int64
		Name string
	}

	db := &DB{Config: &Config{}}

	tests := []struct {
		name string
		fn   func() *builder.QueryBuilder
	}{
		{
			name: "Join",
			fn: func() *builder.QueryBuilder {
				return db.Model(&User{}).Join("profiles", "profiles.id = users.profile_id")
			},
		},
		{
			name: "LeftJoin",
			fn: func() *builder.QueryBuilder {
				return db.Model(&User{}).LeftJoin("profiles", "profiles.id = users.profile_id")
			},
		},
		{
			name: "InnerJoin",
			fn: func() *builder.QueryBuilder {
				return db.Model(&User{}).InnerJoin("profiles", "profiles.id = users.profile_id")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qb := tt.fn()
			if qb == nil {
				t.Error("Join method returned nil")
			}
		})
	}
}

// TestBuilder_Preload tests the Preload method
func TestBuilder_Preload(t *testing.T) {
	type Profile struct {
		ID   int64
		Name string
	}
	type Post struct {
		ID     int64
		UserID int64
		Title  string
	}
	type User struct {
		ID      int64
		Profile *Profile
		Posts   []Post
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Preload("Profile", "Posts")
	if qb == nil {
		t.Error("Preload() returned nil")
	}
}

// TestBuilder_Paginate tests the Paginate method
func TestBuilder_Paginate(t *testing.T) {
	type User struct {
		ID int64
	}

	db := &DB{Config: &Config{}}
	qb := db.Model(&User{}).Paginate(2, 25)
	if qb == nil {
		t.Error("Paginate() returned nil")
	}

	if got := qb.GetLimit(); got != 25 {
		t.Errorf("Paginate() limit = %v, want 25", got)
	}
	if got := qb.GetOffset(); got != 25 {
		t.Errorf("Paginate() offset = %v, want 25", got)
	}
}

// TestBuilder_Raw tests the Raw method
func TestBuilder_Raw(t *testing.T) {
	db := &DB{Config: &Config{}}
	qb := db.Table("users").Raw("SELECT * FROM users WHERE id = ?", 1)
	if qb == nil {
		t.Error("Raw() returned nil")
	}
}

// TestBuilder_Exec tests the Exec method
func TestBuilder_Exec(t *testing.T) {
	db := &DB{Config: &Config{}}
	qb := db.Table("users")

	// This should fail without a real driver
	_, err := qb.Exec("UPDATE users SET name = ?", "John")
	if err == nil {
		t.Error("Exec() should return error for nil driver")
	}
}

// TestSecurity_ValidateRawQuery tests raw query validation
func TestSecurity_ValidateRawQuery(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "Valid SELECT",
			sql:     "SELECT * FROM users WHERE id = ?",
			wantErr: false,
		},
		{
			name:    "SQL injection with DROP",
			sql:     "SELECT * FROM users; DROP TABLE users",
			wantErr: true,
		},
		{
			name:    "SQL injection with comment",
			sql:     "SELECT * FROM users WHERE name = 'admin' --",
			wantErr: true,
		},
		{
			name:    "Multiple statements",
			sql:     "SELECT * FROM users; SELECT * FROM posts",
			wantErr: false, // Warning but not error by default
		},
		{
			name:    "DDL statement",
			sql:     "DROP TABLE users",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := security.ValidateRawQuery(tt.sql, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRawQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestSecurity_ValidateFieldName tests field name validation
func TestSecurity_ValidateFieldName(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		wantErr bool
	}{
		{
			name:    "Valid field name",
			field:   "username",
			wantErr: false,
		},
		{
			name:    "Field with underscore",
			field:   "user_name",
			wantErr: false,
		},
		{
			name:    "Field with dot",
			field:   "profile.name",
			wantErr: false,
		},
		{
			name:    "Field with dangerous pattern",
			field:   "name; DROP TABLE",
			wantErr: true,
		},
		{
			name:    "Field starting with number",
			field:   "1field",
			wantErr: true,
		},
		{
			name:    "Empty field name",
			field:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := security.ValidateFieldName(tt.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFieldName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestSecurity_SanitizeFieldName tests field name sanitization
func TestSecurity_SanitizeFieldName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Clean field name",
			input:    "username",
			expected: "username",
		},
		{
			name:     "Field with special chars",
			input:    "user; DROP TABLE",
			expected: "userDROPTABLE",
		},
		{
			name:     "Field starting with number",
			input:    "123field",
			expected: "_123field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := security.SanitizeFieldName(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeFieldName() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestErrors_IsErrNotFound tests error checking helpers
func TestErrors_IsErrNotFound(t *testing.T) {
	if !IsErrNotFound(ErrNotFound) {
		t.Error("IsErrNotFound(ErrNotFound) = false, want true")
	}
	if IsErrNotFound(ErrDuplicate) {
		t.Error("IsErrNotFound(ErrDuplicate) = true, want false")
	}
}

// TestErrors_DetailedError tests detailed error
func TestErrors_DetailedError(t *testing.T) {
	detailedErr := NewDetailedError("Find", "users", ErrNotFound)
	if detailedErr == nil {
		t.Fatal("NewDetailedError() returned nil")
	}

	if detailedErr.Op != "Find" {
		t.Errorf("DetailedError.Op = %v, want Find", detailedErr.Op)
	}
	if detailedErr.Table != "users" {
		t.Errorf("DetailedError.Table = %v, want users", detailedErr.Table)
	}
	if detailedErr.Err != ErrNotFound {
		t.Errorf("DetailedError.Err = %v, want ErrNotFound", detailedErr.Err)
	}
}

// TestErrors_WithErrorContext tests error with context
func TestErrors_WithErrorContext(t *testing.T) {
	err := WithErrorContext(ErrNotFound, "operation", "Find", "table", "users")
	if err == nil {
		t.Fatal("WithErrorContext() returned nil")
	}

	ctxErr, ok := err.(*ContextError)
	if !ok {
		t.Fatal("WithErrorContext() did not return *ContextError")
	}

	if len(ctxErr.Context) != 2 {
		t.Errorf("ContextError.Context length = %v, want 2", len(ctxErr.Context))
	}
}

// BenchmarkBuilder_Where benchmarks Where method
func BenchmarkBuilder_Where(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID   int64
		Name string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).Where("id = ?", i)
		_ = qb
	}
}

// BenchmarkBuilder_WhereMap benchmarks Where with map
func BenchmarkBuilder_WhereMap(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID   int64
		Name string
		Age  int
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).Where(map[string]interface{}{"age": i})
		_ = qb
	}
}

// BenchmarkBuilder_In benchmarks In method
func BenchmarkBuilder_In(b *testing.B) {
	db := &DB{Config: &Config{}}
	type User struct {
		ID int64
	}

	values := []interface{}{1, 2, 3, 4, 5}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qb := db.Model(&User{}).In("id", values...)
		_ = qb
	}
}
