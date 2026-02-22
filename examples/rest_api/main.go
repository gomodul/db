// Package main is a production-ready REST API example using gomodul/db
// This example demonstrates best practices for building a REST API with the library
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gomodul/db"
	"github.com/gomodul/db/migrate"
)

// ============================================================================
// Domain Models
// ============================================================================

// User represents a user in the system
type User struct {
	ID        int64     `json:"id" db:"id,pk,autoIncrement"`
	Name      string    `json:"name" db:"name,size:100,notnull"`
	Email     string    `json:"email" db:"email,unique,size:255,notnull"`
	Password  string    `json:"-" db:"password,size:255,notnull"` // Never expose in JSON
	Age       int       `json:"age" db:"age"`
	Status    string    `json:"status" db:"status,default:'active',size:20"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// TableName specifies the table name for User model
func (User) TableName() string {
	return "users"
}

// Post represents a blog post
type Post struct {
	ID        int64     `json:"id" db:"id,pk,autoIncrement"`
	Title     string    `json:"title" db:"title,size:255,notnull"`
	Content   string    `json:"content" db:"content,type:TEXT"`
	AuthorID  int64     `json:"author_id" db:"author_id,notnull"`
	Status    string    `json:"status" db:"status,default:'draft',size:20"`
	Published bool      `json:"published" db:"published,default:false"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`

	// Relationships (not persisted)
	Author *User `json:"author,omitempty" db:"-"`
}

// TableName specifies the table name for Post model
func (Post) TableName() string {
	return "posts"
}

// Comment represents a comment on a post
type Comment struct {
	ID        int64     `json:"id" db:"id,pk,autoIncrement"`
	PostID    int64     `json:"post_id" db:"post_id,notnull"`
	AuthorID  int64     `json:"author_id" db:"author_id,notnull"`
	Content   string    `json:"content" db:"content,size:1000,notnull"`
	Status    string    `json:"status" db:"status,default:'pending',size:20"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// ============================================================================
// Request/Response DTOs
// ============================================================================

// CreateUserRequest represents a request to create a user
type CreateUserRequest struct {
	Name     string `json:"name" validate:"required,min=3,max=100"`
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required,min=8"`
	Age      int    `json:"age" validate:"gte=0,lte=150"`
}

// UpdateUserRequest represents a request to update a user
type UpdateUserRequest struct {
	Name  string `json:"name" validate:"min=3,max=100"`
	Email string `json:"email" validate:"email"`
	Age   int    `json:"age" validate:"gte=0,lte=150"`
}

// ListUsersRequest represents query parameters for listing users
type ListUsersRequest struct {
	Page      int    `json:"page" validate:"gte=1"`
	PageSize  int    `json:"page_size" validate:"gte=1,lte=100"`
	Status    string `json:"status" validate:"omitempty,oneof=active inactive suspended"`
	Search    string `json:"search"`
	SortBy    string `json:"sort_by" validate:"omitempty,oneof=name created_at updated_at"`
	SortOrder string `json:"sort_order" validate:"omitempty,oneof=asc desc"`
}

// UserListResponse represents a paginated list of users
type UserListResponse struct {
	Users      []User `json:"users"`
	Total      int64  `json:"total"`
	Page       int    `json:"page"`
	PageSize   int    `json:"page_size"`
	TotalPages int    `json:"total_pages"`
}

// CreatePostRequest represents a request to create a post
type CreatePostRequest struct {
	Title    string `json:"title" validate:"required,min=3,max=255"`
	Content  string `json:"content" validate:"required"`
	AuthorID int64  `json:"author_id" validate:"required"`
	Status   string `json:"status" validate:"omitempty,oneof=draft published archived"`
}

// ============================================================================
// Repository Layer
// ============================================================================

// UserRepository handles user data operations
type UserRepository struct {
	db *db.DB
}

// NewUserRepository creates a new user repository
func NewUserRepository(database *db.DB) *UserRepository {
	return &UserRepository{db: database}
}

// Create creates a new user
func (r *UserRepository) Create(ctx context.Context, user *User) error {
	return r.db.Model(&User{}).Context(ctx).Create(user)
}

// FindByID finds a user by ID
func (r *UserRepository) FindByID(ctx context.Context, id int64) (*User, error) {
	var user User
	err := r.db.Model(&User{}).
		Context(ctx).
		Where("id = ?", id).
		First(&user)

	if err != nil {
		return nil, err
	}
	return &user, nil
}

// FindByEmail finds a user by email
func (r *UserRepository) FindByEmail(ctx context.Context, email string) (*User, error) {
	var user User
	err := r.db.Model(&User{}).
		Context(ctx).
		Where("email = ?", email).
		First(&user)

	if err != nil {
		return nil, err
	}
	return &user, nil
}

// List lists users with pagination and filtering
func (r *UserRepository) List(ctx context.Context, req *ListUsersRequest) (*UserListResponse, error) {
	// Count total matching records
	countQuery := r.db.Model(&User{}).Context(ctx)

	if req.Status != "" {
		countQuery = countQuery.Where("status = ?", req.Status)
	}
	if req.Search != "" {
		countQuery = countQuery.Where("name LIKE ?", "%"+req.Search+"%")
	}

	total, err := countQuery.Count()
	if err != nil {
		return nil, err
	}

	// Get paginated results
	query := r.db.Model(&User{}).Context(ctx)

	if req.Status != "" {
		query = query.Where("status = ?", req.Status)
	}
	if req.Search != "" {
		query = query.Where("name LIKE ?", "%"+req.Search+"%")
	}

	// Apply sorting
	sortBy := "created_at"
	if req.SortBy != "" {
		sortBy = req.SortBy
	}
	sortOrder := "DESC"
	if req.SortOrder != "" {
		sortOrder = req.SortOrder
	}
	query = query.Order(fmt.Sprintf("%s %s", sortBy, sortOrder))

	// Apply pagination
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}
	query = query.Paginate(req.Page, req.PageSize)

	var users []User
	if err := query.Find(&users); err != nil {
		return nil, err
	}

	// Calculate total pages
	totalPages := int(total) / req.PageSize
	if int(total)%req.PageSize != 0 {
		totalPages++
	}

	return &UserListResponse{
		Users:      users,
		Total:      total,
		Page:       req.Page,
		PageSize:   req.PageSize,
		TotalPages: totalPages,
	}, nil
}

// Update updates a user
func (r *UserRepository) Update(ctx context.Context, user *User) error {
	updates := map[string]interface{}{
		"name":       user.Name,
		"email":      user.Email,
		"age":        user.Age,
		"status":     user.Status,
		"updated_at": time.Now(),
	}
	return r.db.Model(&User{}).
		Context(ctx).
		Where("id = ?", user.ID).
		Update(updates)
}

// Delete soft deletes a user
func (r *UserRepository) Delete(ctx context.Context, id int64) error {
	return r.db.Model(&User{}).
		Context(ctx).
		Where("id = ?", id).
		Update(map[string]interface{}{
			"status":     "deleted",
			"updated_at": time.Now(),
		})
}

// PostRepository handles post data operations
type PostRepository struct {
	db *db.DB
}

// NewPostRepository creates a new post repository
func NewPostRepository(database *db.DB) *PostRepository {
	return &PostRepository{db: database}
}

// Create creates a new post
func (r *PostRepository) Create(ctx context.Context, post *Post) error {
	return r.db.Model(&Post{}).Context(ctx).Create(post)
}

// FindByID finds a post with its author
func (r *PostRepository) FindByID(ctx context.Context, id int64) (*Post, error) {
	var post Post
	err := r.db.Model(&Post{}).
		Context(ctx).
		Where("id = ?", id).
		First(&post)

	if err != nil {
		return nil, err
	}

	// Load author relationship if needed
	if post.AuthorID > 0 {
		var author User
		if err := r.db.Model(&User{}).
			Context(ctx).
			Where("id = ?", post.AuthorID).
			First(&author); err == nil {
			post.Author = &author
		}
	}

	return &post, nil
}

// List lists posts with pagination
func (r *PostRepository) List(ctx context.Context, page, pageSize int, status string) ([]Post, int64, error) {
	query := r.db.Model(&Post{}).Context(ctx)

	if status != "" {
		query = query.Where("status = ?", status)
	}

	// Count total
	total, err := query.Count()
	if err != nil {
		return nil, 0, err
	}

	// Get paginated results
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}

	query = query.Paginate(page, pageSize).Order("created_at DESC")

	var posts []Post
	if err := query.Find(&posts); err != nil {
		return nil, 0, err
	}

	return posts, total, nil
}

// ============================================================================
// Service Layer
// ============================================================================

// UserService handles user business logic
type UserService struct {
	repo *UserRepository
}

// NewUserService creates a new user service
func NewUserService(repo *UserRepository) *UserService {
	return &UserService{repo: repo}
}

// CreateUser creates a new user with validation
func (s *UserService) CreateUser(ctx context.Context, req *CreateUserRequest) (*User, error) {
	// Check if email already exists
	existing, err := s.repo.FindByEmail(ctx, req.Email)
	if err == nil && existing != nil {
		return nil, fmt.Errorf("email already exists")
	}

	// Hash password (in production, use bcrypt)
	hashedPassword := hashPassword(req.Password)

	user := &User{
		Name:      req.Name,
		Email:     req.Email,
		Password:  hashedPassword,
		Age:       req.Age,
		Status:    "active",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.repo.Create(ctx, user); err != nil {
		return nil, err
	}

	return user, nil
}

// GetUser retrieves a user by ID
func (s *UserService) GetUser(ctx context.Context, id int64) (*User, error) {
	return s.repo.FindByID(ctx, id)
}

// ListUsers retrieves a paginated list of users
func (s *UserService) ListUsers(ctx context.Context, req *ListUsersRequest) (*UserListResponse, error) {
	return s.repo.List(ctx, req)
}

// UpdateUser updates a user
func (s *UserService) UpdateUser(ctx context.Context, id int64, req *UpdateUserRequest) (*User, error) {
	user, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Update fields
	if req.Name != "" {
		user.Name = req.Name
	}
	if req.Email != "" {
		user.Email = req.Email
	}
	if req.Age > 0 {
		user.Age = req.Age
	}

	if err := s.repo.Update(ctx, user); err != nil {
		return nil, err
	}

	return user, nil
}

// DeleteUser deletes a user
func (s *UserService) DeleteUser(ctx context.Context, id int64) error {
	return s.repo.Delete(ctx, id)
}

// PostService handles post business logic
type PostService struct {
	postRepo *PostRepository
	userRepo *UserRepository
}

// NewPostService creates a new post service
func NewPostService(postRepo *PostRepository, userRepo *UserRepository) *PostService {
	return &PostService{
		postRepo: postRepo,
		userRepo: userRepo,
	}
}

// CreatePost creates a new post
func (s *PostService) CreatePost(ctx context.Context, req *CreatePostRequest) (*Post, error) {
	// Verify author exists
	author, err := s.userRepo.FindByID(ctx, req.AuthorID)
	if err != nil || author == nil {
		return nil, fmt.Errorf("author not found")
	}

	post := &Post{
		Title:     req.Title,
		Content:   req.Content,
		AuthorID:  req.AuthorID,
		Status:    req.Status,
		Published: req.Status == "published",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.postRepo.Create(ctx, post); err != nil {
		return nil, err
	}

	// Load author for response
	post.Author = author

	return post, nil
}

// GetPost retrieves a post by ID
func (s *PostService) GetPost(ctx context.Context, id int64) (*Post, error) {
	return s.postRepo.FindByID(ctx, id)
}

// GetPosts retrieves a paginated list of posts
func (s *PostService) GetPosts(ctx context.Context, page, pageSize int, status string) ([]Post, int64, error) {
	return s.postRepo.List(ctx, page, pageSize, status)
}

// ============================================================================
// HTTP Handlers
// ============================================================================

// Server represents the HTTP server
type Server struct {
	db          *db.DB
	userService *UserService
	postService *PostService
	router      *http.ServeMux
}

// NewServer creates a new server
func NewServer(database *db.DB) *Server {
	userRepo := NewUserRepository(database)
	postRepo := NewPostRepository(database)

	server := &Server{
		db:          database,
		userService: NewUserService(userRepo),
		postService: NewPostService(postRepo, userRepo),
		router:      http.NewServeMux(),
	}

	server.setupRoutes()
	return server
}

// setupRoutes configures all routes
func (s *Server) setupRoutes() {
	// User routes
	s.router.HandleFunc("/api/v1/users", s.handleUsers)
	s.router.HandleFunc("/api/v1/users/", s.handleUserByID)

	// Post routes
	s.router.HandleFunc("/api/v1/posts", s.handlePosts)
	s.router.HandleFunc("/api/v1/posts/", s.handlePostByID)

	// Health check
	s.router.HandleFunc("/health", s.healthCheck)
}

// createUser handles POST /users
func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, err)
		return
	}

	user, err := s.userService.CreateUser(ctx, &req)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err)
		return
	}

	respondJSON(w, http.StatusCreated, user)
}

// listUsers handles GET /users
func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req := parseListUsersRequest(r)
	users, err := s.userService.ListUsers(ctx, req)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err)
		return
	}

	respondJSON(w, http.StatusOK, users)
}

// createPost handles POST /posts
func (s *Server) createPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req CreatePostRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, err)
		return
	}

	post, err := s.postService.CreatePost(ctx, &req)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err)
		return
	}

	respondJSON(w, http.StatusCreated, post)
}

// listPosts handles GET /posts
func (s *Server) listPosts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	page := parseIntQuery(r, "page", 1)
	pageSize := parseIntQuery(r, "page_size", 20)
	status := r.URL.Query().Get("status")

	posts, total, err := s.postService.GetPosts(ctx, page, pageSize, status)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err)
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"posts": posts,
		"total": total,
		"page":  page,
	})
}

// healthCheck handles GET /health
func (s *Server) healthCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	health, err := s.db.GetPoolHealth(ctx)
	if err != nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}

	status := http.StatusOK
	if !health.Healthy {
		status = http.StatusServiceUnavailable
	}

	respondJSON(w, status, map[string]interface{}{
		"status":   "healthy",
		"database": health.Healthy,
	})
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// ============================================================================
// Helper Functions
// ============================================================================

// respondJSON sends a JSON response
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// respondError sends an error response
func respondError(w http.ResponseWriter, status int, err error) {
	respondJSON(w, status, map[string]interface{}{
		"error": err.Error(),
	})
}

// parseListUsersRequest parses query parameters for listing users
func parseListUsersRequest(r *http.Request) *ListUsersRequest {
	req := &ListUsersRequest{
		Page:      parseIntQuery(r, "page", 1),
		PageSize:  parseIntQuery(r, "page_size", 20),
		Status:    r.URL.Query().Get("status"),
		Search:    r.URL.Query().Get("search"),
		SortBy:    r.URL.Query().Get("sort_by"),
		SortOrder: r.URL.Query().Get("sort_order"),
	}

	// Apply defaults
	if req.SortOrder == "" {
		req.SortOrder = "desc"
	}

	return req
}

// parseIntQuery parses an integer query parameter
func parseIntQuery(r *http.Request, key string, defaultVal int) int {
	if val := r.URL.Query().Get(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}

// hashPassword hashes a password (in production, use bcrypt)
func hashPassword(password string) string {
	// In production, use bcrypt:
	// hashed, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	// return string(hashed)
	return password // Simplified for example
}

// extractID extracts ID from URL path /api/v1/users/{id}
func extractID(path string) (int64, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 5 {
		return 0, fmt.Errorf("invalid path")
	}
	return strconv.ParseInt(parts[4], 10, 64)
}

// handleUsers handles GET/POST /users
func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.listUsers(w, r)
	case "POST":
		s.createUser(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleUserByID handles GET/PUT/DELETE /users/{id}
func (s *Server) handleUserByID(w http.ResponseWriter, r *http.Request) {
	id, err := extractID(r.URL.Path)
	if err != nil {
		respondError(w, http.StatusBadRequest, err)
		return
	}

	switch r.Method {
	case "GET":
		// Inject ID into request context for the handler
		s.getUserByID(w, r, id)
	case "PUT":
		s.updateUser(w, r, id)
	case "DELETE":
		s.deleteUser(w, r, id)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePosts handles GET/POST /posts
func (s *Server) handlePosts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.listPosts(w, r)
	case "POST":
		s.createPost(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePostByID handles GET /posts/{id}
func (s *Server) handlePostByID(w http.ResponseWriter, r *http.Request) {
	id, err := extractID(r.URL.Path)
	if err != nil {
		respondError(w, http.StatusBadRequest, err)
		return
	}
	s.getPost(w, r, id)
}

// getUserByID is the internal handler for GET /users/{id}
func (s *Server) getUserByID(w http.ResponseWriter, r *http.Request, id int64) {
	ctx := r.Context()
	user, err := s.userService.GetUser(ctx, id)
	if err != nil {
		respondError(w, http.StatusNotFound, err)
		return
	}
	respondJSON(w, http.StatusOK, user)
}

// updateUser is the internal handler for PUT /users/{id}
func (s *Server) updateUser(w http.ResponseWriter, r *http.Request, id int64) {
	ctx := r.Context()
	var req UpdateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, err)
		return
	}
	user, err := s.userService.UpdateUser(ctx, id, &req)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err)
		return
	}
	respondJSON(w, http.StatusOK, user)
}

// deleteUser is the internal handler for DELETE /users/{id}
func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request, id int64) {
	ctx := r.Context()
	if err := s.userService.DeleteUser(ctx, id); err != nil {
		respondError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// getPost is the internal handler for GET /posts/{id}
func (s *Server) getPost(w http.ResponseWriter, r *http.Request, id int64) {
	ctx := r.Context()
	post, err := s.postService.GetPost(ctx, id)
	if err != nil {
		respondError(w, http.StatusNotFound, err)
		return
	}
	respondJSON(w, http.StatusOK, post)
}

// ============================================================================
// Main
// ============================================================================

func main() {
	// Initialize database
	database, err := db.Open(db.Config{
		DSN:            "postgres://user:pass@localhost:5432/mydb",
		MaxOpenConns:   25,
		MaxIdleConns:   5,
		ConnMaxLifetime: 30 * time.Minute,
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	// Auto-migrate schema
	if err := database.AutoMigrate(&User{}, &Post{}, &Comment{}); err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	// Enable pool monitoring
	database.EnablePoolMonitoring(nil)

	// Create server
	server := NewServer(database)

	// Start HTTP server
	addr := ":8080"
	log.Printf("Starting server on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server))
}

// ============================================================================
// Migration Utility
// ============================================================================

// MigrateSchema runs database migrations
func MigrateSchema(database *db.DB) error {
	migrator := database.Migrator()
	ctx := context.Background()

	// Create tables
	if err := migrator.AutoMigrate(ctx, &User{}, &Post{}, &Comment{}); err != nil {
		return err
	}

	// Create indexes using AddIndex which handles model to table name conversion
	if err := migrator.AddIndex(ctx, &User{}, &migrate.IndexInfo{
		Name:    "idx_user_email_status",
		Columns: []string{"email", "status"},
		Unique:  false,
	}); err != nil {
		return err
	}

	if err := migrator.AddIndex(ctx, &Post{}, &migrate.IndexInfo{
		Name:    "idx_post_author_status",
		Columns: []string{"author_id", "status"},
		Unique:  false,
	}); err != nil {
		return err
	}

	return nil
}
