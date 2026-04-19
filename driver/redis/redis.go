package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// Driver implements the dialect.Driver interface for Redis
type Driver struct {
	client *redis.Client
	config *dialect.Config
}

// NewDriver creates a new Redis driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "redis"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeKVStore
}

// Initialize initializes the Redis connection
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg

	opt, err := redis.ParseURL(cfg.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse redis DSN: %w", err)
	}

	client := redis.NewClient(opt)

	// Ping the database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping error: %w", err)
	}

	d.client = client
	return nil
}

// Close closes the Redis connection
func (d *Driver) Close() error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}

// Execute executes a universal query
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	switch q.Operation {
	case query.OpFind:
		return d.executeGet(ctx, q)
	case query.OpCreate:
		return d.executeSet(ctx, q)
	case query.OpUpdate:
		return d.executeSet(ctx, q) // Redis SET is same as update
	case query.OpDelete:
		return d.executeDelete(ctx, q)
	case query.OpCount:
		return d.executeCount(ctx, q)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", q.Operation)
	}
}

// executeGet executes a GET command
func (d *Driver) executeGet(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	key := d.getKey(q)

	// For simple key retrieval
	if len(q.Filters) == 0 || len(q.Filters) == 1 && q.Filters[0].Field == "key" {
		val, err := d.client.Get(ctx, key).Result()
		if err != nil {
			if err == redis.Nil {
				return &dialect.Result{Data: []interface{}{}}, nil
			}
			return nil, err
		}
		return &dialect.Result{
			Data:        []interface{}{map[string]interface{}{"key": key, "value": val}},
			RowsAffected: 1,
		}, nil
	}

	// For pattern matching (SCAN)
	if pattern := d.getPatternFromFilters(q.Filters); pattern != "" {
		var results []interface{}
		iter := d.client.Scan(ctx, 0, pattern, 0).Iterator()
		for iter.Next(ctx) {
			k := iter.Val()
			val, _ := d.client.Get(ctx, k).Result()
			results = append(results, map[string]interface{}{"key": k, "value": val})
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}
		return &dialect.Result{
			Data:        results,
			RowsAffected: int64(len(results)),
		}, nil
	}

	return &dialect.Result{Data: []interface{}{}}, nil
}

// executeSet executes a SET command
func (d *Driver) executeSet(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	key := d.getKey(q)
	value := d.getValue(q)

	// Check for expiration
	var expiration time.Duration
	if ttl, ok := q.Hints["ttl"].(time.Duration); ok {
		expiration = ttl
	}

	err := d.client.Set(ctx, key, value, expiration).Err()
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		RowsAffected:  1,
		LastInsertID: key,
	}, nil
}

// executeDelete executes a DEL command
func (d *Driver) executeDelete(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	key := d.getKey(q)

	count, err := d.client.Del(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	return &dialect.Result{
		RowsAffected: count,
	}, nil
}

// executeCount counts keys matching pattern
func (d *Driver) executeCount(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	pattern := d.getPatternFromFilters(q.Filters)
	if pattern == "" {
		pattern = "*"
	}

	var count int64
	iter := d.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		count++
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return &dialect.Result{
		Count: count,
	}, nil
}

// Capabilities returns the driver's capabilities
func (d *Driver) Capabilities() *dialect.Capabilities {
	return &dialect.Capabilities{
		Query: dialect.QueryCapabilities{
			Create: true,
			Read:   true,
			Update: true,
			Delete: true,
			Filters: []query.FilterOperator{
				query.OpEqual,
				query.OpNotEqual,
				query.OpContains,
				query.OpStartsWith,
				query.OpEndsWith,
			},
			FullTextSearch: true,
		},
	}
}

// Ping checks if Redis is reachable
func (d *Driver) Ping(ctx context.Context) error {
	return d.client.Ping(ctx).Err()
}

// Health returns the health status
func (d *Driver) Health() (*dialect.HealthStatus, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.Ping(ctx); err != nil {
		return dialect.NewUnhealthyStatus(err.Error()), nil
	}

	// Get Redis info
	info, err := d.client.Info(ctx, "server").Result()
	if err != nil {
		return dialect.NewHealthyStatus(time.Since(start)), nil
	}

	return dialect.NewHealthyStatus(time.Since(start)).WithDetail("info", info), nil
}

// BeginTx starts a new transaction (Redis MULTI/EXEC)
func (d *Driver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	return &RedisTx{ctx: ctx, driver: d}, nil
}

// RedisTx represents a Redis transaction
type RedisTx struct {
	ctx    context.Context
	driver *Driver
	cmds   []redis.Cmder
}

// Commit commits the transaction (EXEC)
func (t *RedisTx) Commit() error {
	// Redis transactions use pipelining
	pipe := t.driver.client.Pipeline()
	for _, cmd := range t.cmds {
		pipe.Process(t.ctx, cmd)
	}
	_, err := pipe.Exec(t.ctx)
	return err
}

// Rollback rolls back the transaction (DISCARD)
func (t *RedisTx) Rollback() error {
	t.cmds = nil
	return nil
}

// Query executes a query within the transaction
func (t *RedisTx) Query(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	return t.driver.Execute(ctx, q)
}

// Exec executes a command within the transaction
func (t *RedisTx) Exec(ctx context.Context, rawSQL string, args ...interface{}) (*dialect.Result, error) {
	cmd := redis.NewCmd(ctx, append([]interface{}{rawSQL}, args...)...)
	t.cmds = append(t.cmds, cmd)
	return &dialect.Result{}, nil
}

// Helper methods

func (d *Driver) getKey(q *query.Query) string {
	// Try to get key from filters
	for _, filter := range q.Filters {
		if filter.Field == "key" {
			return fmt.Sprintf("%v", filter.Value)
		}
	}
	// Use collection as key prefix
	if q.Collection != "" {
		return q.Collection
	}
	return "default"
}

func (d *Driver) getValue(q *query.Query) interface{} {
	if q.Document != nil {
		if m, ok := q.Document.(map[string]interface{}); ok {
			if val, exists := m["value"]; exists {
				return val
			}
		}
		return q.Document
	}

	// Try to get value from updates
	for k, v := range q.Updates {
		if k == "value" {
			return v
		}
	}

	return ""
}

func (d *Driver) getPatternFromFilters(filters []*query.Filter) string {
	for _, filter := range filters {
		if filter.Field == "key" {
			switch filter.Operator {
			case query.OpContains, query.OpLike:
				return fmt.Sprintf("*%v*", filter.Value)
			case query.OpStartsWith:
				return fmt.Sprintf("%v*", filter.Value)
			case query.OpEndsWith:
				return fmt.Sprintf("*%v", filter.Value)
			case query.OpEqual:
				return fmt.Sprintf("%v", filter.Value)
			}
		}
	}
	return ""
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case time.Time:
		return strconv.FormatInt(val.Unix(), 10)
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Migrator returns the dialect.Migrator for schema operations
func (d *Driver) Migrator() dialect.Migrator {
	return &Migrator{driver: d}
}

func init() {
	// Register the Redis driver
	dialect.Register("redis", func() dialect.Driver {
		return NewDriver()
	})
}
