package shard

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/gomodul/db"
)

// Strategy defines how to route data to shards
type Strategy interface {
	// GetShard returns the shard ID for a given model
	GetShard(model interface{}) (string, error)

	// GetAllShards returns all available shard IDs
	GetAllShards() []string

	// Name returns the strategy name
	Name() string
}

// HashStrategy uses hash-based sharding
type HashStrategy struct {
	shardCount int
	shardIDs   []string
}

// NewHashStrategy creates a new hash-based sharding strategy
func NewHashStrategy(shardIDs []string) *HashStrategy {
	return &HashStrategy{
		shardCount: len(shardIDs),
		shardIDs:   shardIDs,
	}
}

// GetShard returns the shard ID using hash-based routing
func (s *HashStrategy) GetShard(model interface{}) (string, error) {
	// Generate hash from model
	hash := s.hashModel(model)
	index := hash % uint32(s.shardCount)
	return s.shardIDs[index], nil
}

// GetAllShards returns all shard IDs
func (s *HashStrategy) GetAllShards() []string {
	return s.shardIDs
}

// Name returns the strategy name
func (s *HashStrategy) Name() string {
	return "hash"
}

// hashModel generates a hash from a model
func (s *HashStrategy) hashModel(model interface{}) uint32 {
	// Simple hash implementation - in production, use a more robust method
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", model)))
	return h.Sum32()
}

// RangeStrategy uses range-based sharding
type RangeStrategy struct {
	ranges map[Range]string
}

// Range represents a value range for sharding
type Range struct {
	Min interface{}
	Max interface{}
}

// NewRangeStrategy creates a new range-based sharding strategy
func NewRangeStrategy() *RangeStrategy {
	return &RangeStrategy{
		ranges: make(map[Range]string),
	}
}

// AddRange adds a range to the strategy
func (s *RangeStrategy) AddRange(shardID string, min, max interface{}) {
	s.ranges[Range{Min: min, Max: max}] = shardID
}

// GetShard returns the shard ID using range-based routing
func (s *RangeStrategy) GetShard(model interface{}) (string, error) {
	// Extract the shard key from the model
	key, err := extractShardKey(model)
	if err != nil {
		return "", err
	}

	// Find the matching range
	for r, shardID := range s.ranges {
		if inRange(key, r.Min, r.Max) {
			return shardID, nil
		}
	}

	return "", fmt.Errorf("no matching range for key: %v", key)
}

// GetAllShards returns all shard IDs
func (s *RangeStrategy) GetAllShards() []string {
	shardIDs := make(map[string]bool)
	for _, shardID := range s.ranges {
		shardIDs[shardID] = true
	}

	ids := make([]string, 0, len(shardIDs))
	for id := range shardIDs {
		ids = append(ids, id)
	}
	return ids
}

// Name returns the strategy name
func (s *RangeStrategy) Name() string {
	return "range"
}

// ConsistentHashStrategy uses consistent hashing for shard distribution
type ConsistentHashStrategy struct {
	hashRing map[uint32]string
	sortedHashes []uint32
	shardIDs []string
	virtualNodes int
}

// NewConsistentHashStrategy creates a new consistent hash strategy
func NewConsistentHashStrategy(shardIDs []string, virtualNodes int) *ConsistentHashStrategy {
	s := &ConsistentHashStrategy{
		hashRing: make(map[uint32]string),
		shardIDs: shardIDs,
		virtualNodes: virtualNodes,
	}
	s.buildHashRing()
	return s
}

// GetShard returns the shard ID using consistent hashing
func (s *ConsistentHashStrategy) GetShard(model interface{}) (string, error) {
	hash := s.hashModel(model)

	// Find the first node with hash >= model hash
	for _, nodeHash := range s.sortedHashes {
		if nodeHash >= hash {
			return s.hashRing[nodeHash], nil
		}
	}

	// Wrap around to the first node
	return s.hashRing[s.sortedHashes[0]], nil
}

// GetAllShards returns all shard IDs
func (s *ConsistentHashStrategy) GetAllShards() []string {
	return s.shardIDs
}

// Name returns the strategy name
func (s *ConsistentHashStrategy) Name() string {
	return "consistent_hash"
}

// buildHashRing builds the consistent hash ring
func (s *ConsistentHashStrategy) buildHashRing() {
	h := fnv.New32a()

	for _, shardID := range s.shardIDs {
		for i := 0; i < s.virtualNodes; i++ {
			h.Write([]byte(fmt.Sprintf("%s:%d", shardID, i)))
			hash := h.Sum32()
			s.hashRing[hash] = shardID
			s.sortedHashes = append(s.sortedHashes, hash)
			h.Reset()
		}
	}

	// Sort the hashes
	s.sortHashes()
}

// hashModel generates a hash from a model
func (s *ConsistentHashStrategy) hashModel(model interface{}) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", model)))
	return h.Sum32()
}

// sortHashes sorts the hash ring (simple bubble sort)
func (s *ConsistentHashStrategy) sortHashes() {
	n := len(s.sortedHashes)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if s.sortedHashes[j] > s.sortedHashes[j+1] {
				s.sortedHashes[j], s.sortedHashes[j+1] = s.sortedHashes[j+1], s.sortedHashes[j]
			}
		}
	}
}

// ============ Shard Manager ============

// Manager manages database shards
type Manager struct {
	mu         sync.RWMutex
	shards     map[string]*db.DB
	strategies map[string]Strategy // model name -> strategy
	keyField   string // Field to use for sharding (e.g., "user_id")
}

// NewManager creates a new shard manager
func NewManager() *Manager {
	return &Manager{
		shards:     make(map[string]*db.DB),
		strategies: make(map[string]Strategy),
		keyField:   "id",
	}
}

// AddShard adds a database shard
func (m *Manager) AddShard(shardID string, database *db.DB) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.shards[shardID]; exists {
		return fmt.Errorf("shard %s already exists", shardID)
	}

	m.shards[shardID] = database
	return nil
}

// RemoveShard removes a database shard
func (m *Manager) RemoveShard(shardID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.shards[shardID]; !exists {
		return fmt.Errorf("shard %s not found", shardID)
	}

	delete(m.shards, shardID)
	return nil
}

// RegisterStrategy registers a sharding strategy for a model
func (m *Manager) RegisterStrategy(modelName string, strategy Strategy) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.strategies[modelName] = strategy
}

// GetShardForModel returns the shard for a given model
func (m *Manager) GetShardForModel(modelName string, model interface{}) (*db.DB, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	strategy, ok := m.strategies[modelName]
	if !ok {
		return nil, fmt.Errorf("no strategy registered for model %s", modelName)
	}

	shardID, err := strategy.GetShard(model)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard: %w", err)
	}

	shard, ok := m.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	return shard, nil
}

// GetShardByID returns a shard by its ID
func (m *Manager) GetShardByID(shardID string) (*db.DB, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	shard, ok := m.shards[shardID]
	return shard, ok
}

// GetAllShards returns all shard IDs
func (m *Manager) GetAllShards() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.shards))
	for id := range m.shards {
		ids = append(ids, id)
	}
	return ids
}

// SetKeyField sets the field to use for sharding
func (m *Manager) SetKeyField(field string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.keyField = field
}

// GetKeyField returns the field used for sharding
func (m *Manager) GetKeyField() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.keyField
}

// BroadcastQuery executes a query on all shards and returns all results
func (m *Manager) BroadcastQuery(modelName string, queryFn func(*db.DB) (interface{}, error)) ([]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	strategy, ok := m.strategies[modelName]
	if !ok {
		return nil, fmt.Errorf("no strategy registered for model %s", modelName)
	}

	var results []interface{}
	var errs []error

	for _, shardID := range strategy.GetAllShards() {
		shard, ok := m.shards[shardID]
		if !ok {
			errs = append(errs, fmt.Errorf("shard %s not found", shardID))
			continue
		}

		result, err := queryFn(shard)
		if err != nil {
			errs = append(errs, fmt.Errorf("shard %s error: %w", shardID, err))
			continue
		}

		results = append(results, result)
	}

	if len(errs) > 0 {
		return results, fmt.Errorf("broadcast query completed with errors: %v", errs)
	}

	return results, nil
}

// ============ Helper Functions ============

// extractShardKey extracts the shard key from a model
func extractShardKey(model interface{}) (interface{}, error) {
	// This would need to be implemented based on your model structure
	// For now, return a placeholder
	return "key", nil
}

// inRange checks if a value is within a range
func inRange(value, min, max interface{}) bool {
	// Simple comparison - in production, handle different types
	v, ok1 := value.(int)
	minVal, ok2 := min.(int)
	maxVal, ok3 := max.(int)

	if ok1 && ok2 && ok3 {
		return v >= minVal && v <= maxVal
	}

	return false
}

// ============ Cross-Shard Operations ============

// QueryAllShards executes a query on all shards and returns combined results
// This is useful for queries that need to scan all shards
func (m *Manager) QueryAllShards(modelName string, queryFn func(*db.DB) (interface{}, error)) ([]interface{}, error) {
	return m.BroadcastQuery(modelName, queryFn)
}

// QueryByShard executes a query on a specific shard
func (m *Manager) QueryByShard(shardID string, queryFn func(*db.DB) (interface{}, error)) (interface{}, error) {
	shard, ok := m.GetShardByID(shardID)
	if !ok {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	return queryFn(shard)
}

// AggregateAcrossShards executes aggregations across all shards
// Useful for COUNT, SUM, AVG operations across sharded data
func (m *Manager) AggregateAcrossShards(modelName string, aggregateFn func(*db.DB) (int64, error)) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	strategy, ok := m.strategies[modelName]
	if !ok {
		return 0, fmt.Errorf("no strategy registered for model %s", modelName)
	}

	var total int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	errChan := make(chan error, len(strategy.GetAllShards()))

	for _, shardID := range strategy.GetAllShards() {
		shard, ok := m.shards[shardID]
		if !ok {
			continue
		}

		wg.Add(1)
		go func(db *db.DB) {
			defer wg.Done()

			count, err := aggregateFn(db)
			if err != nil {
				errChan <- err
				return
			}

			mu.Lock()
			total += count
			mu.Unlock()
		}(shard)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var errs []error
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return total, fmt.Errorf("aggregation completed with errors: %v", errs)
	}

	return total, nil
}

// WriteToShard routes a model to the appropriate shard and executes the write operation
func (m *Manager) WriteToShard(modelName string, model interface{}, writeFn func(*db.DB, interface{}) error) error {
	shard, err := m.GetShardForModel(modelName, model)
	if err != nil {
		return fmt.Errorf("failed to get shard for model: %w", err)
	}

	return writeFn(shard, model)
}

// ReadFromShard routes a model to the appropriate shard and executes the read operation
func (m *Manager) ReadFromShard(modelName string, model interface{}, readFn func(*db.DB, interface{}) (interface{}, error)) (interface{}, error) {
	shard, err := m.GetShardForModel(modelName, model)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard for model: %w", err)
	}

	return readFn(shard, model)
}

// GetShardStats returns statistics about all shards
func (m *Manager) GetShardStats() map[string]*ShardStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]*ShardStats)
	for shardID, db := range m.shards {
		// Get basic stats from the database
		stats[shardID] = &ShardStats{
			ShardID: shardID,
			Status:  "unknown",
		}

		// Try to ping the database
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		if err := db.Ping(ctx); err == nil {
			stats[shardID].Status = "healthy"
		} else {
			stats[shardID].Status = "unhealthy"
			stats[ shardID].Error = err.Error()
		}
		cancel()
	}

	return stats
}

// Rebalance redistributes data across shards based on a new strategy
// This is a potentially expensive operation that should be run during maintenance windows
func (m *Manager) Rebalance(modelName string, newStrategy Strategy) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldStrategy, ok := m.strategies[modelName]
	if !ok {
		return fmt.Errorf("no strategy found for model %s", modelName)
	}

	// Get all data from old shards
	oldData := make(map[string][]interface{})
	for _, shardID := range oldStrategy.GetAllShards() {
		_, ok := m.shards[shardID]
		if !ok {
			continue
		}

		// In production, you'd load data from the shard
		// For now, this is a placeholder
		oldData[shardID] = []interface{}{}
	}

	// Update strategy
	m.strategies[modelName] = newStrategy

	// Write data to new shards
	// In production, you'd write data to the new shards based on the new strategy

	// Clean up old data
	// In production, you'd delete data from old shards after verification

	return nil
}

// ShardStats represents statistics about a shard
type ShardStats struct {
	ShardID         string
	Status          string
	Error           string
	ConnectionCount int
	// Add more stats as needed
}

// GetShardInfo returns detailed information about all shards
func (m *Manager) GetShardInfo() map[string]ShardInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info := make(map[string]ShardInfo)
	for shardID := range m.shards {
		info[shardID] = ShardInfo{
			ShardID: shardID,
		}
	}

	return info
}

// ShardInfo represents detailed information about a shard
type ShardInfo struct {
	ShardID    string
	DSN        string
	DriverType string
	Status     string
	Models     []string
}

// Close closes all shard connections
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for shardID, shard := range m.shards {
		if err := shard.Close(); err != nil {
			lastErr = fmt.Errorf("error closing shard %s: %w", shardID, err)
		}
	}

	return lastErr
}

// ============ Helper Functions ============
