package translator

import (
	"fmt"
	"strings"

	"github.com/gomodul/db/query"
)

// RedisTranslator translates universal queries to Redis commands
type RedisTranslator struct {
	*BaseTranslator
	keyPattern string // Pattern for key generation (e.g., "user:{id}")
}

// NewRedisTranslator creates a new Redis translator
func NewRedisTranslator(keyPattern string) *RedisTranslator {
	if keyPattern == "" {
		keyPattern = "{collection}:{id}"
	}
	return &RedisTranslator{
		BaseTranslator: NewBaseTranslator("redis"),
		keyPattern:     keyPattern,
	}
}

// RedisCommand represents a translated Redis command
type RedisCommand struct {
	Cmd  string        // Command name: GET, SET, HGET, etc.
	Args []interface{} // Command arguments
}

// Translate converts universal query to Redis command
func (t *RedisTranslator) Translate(q *query.Query) (BackendQuery, error) {
	if err := t.Validate(q); err != nil {
		return nil, err
	}

	switch q.Operation {
	case query.OpFind:
		return t.translateFind(q)
	case query.OpCreate:
		return t.translateCreate(q)
	case query.OpUpdate:
		return t.translateUpdate(q)
	case query.OpDelete:
		return t.translateDelete(q)
	case query.OpCount:
		return t.translateCount(q)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedOperation, q.Operation)
	}
}

func (t *RedisTranslator) translateFind(q *query.Query) (*RedisCommand, error) {
	// If filtering by ID, use GET
	if idFilter := t.getIDFilter(q.Filters); idFilter != nil {
		key := t.buildKey(q.Collection, idFilter.Value)
		return &RedisCommand{
			Cmd:  "GET",
			Args: []interface{}{key},
		}, nil
	}

	// For complex queries, use SCAN or search
	if len(q.Filters) > 0 {
		// Use SCAN with pattern matching
		pattern := t.buildKey(q.Collection, "*")
		return &RedisCommand{
			Cmd:  "SCAN",
			Args: []interface{}{0, "MATCH", pattern, "COUNT", "100"},
		}, nil
	}

	// For hash-based queries
	return &RedisCommand{
		Cmd:  "HGETALL",
		Args: []interface{}{q.Collection},
	}, nil
}

func (t *RedisTranslator) translateCreate(q *query.Query) (*RedisCommand, error) {
	if q.Document == nil {
		return nil, fmt.Errorf("%w: document required for create", ErrInvalidQuery)
	}

	key := t.generateKey(q.Collection, q.Document)

	// Check if document is a map (for HMSET)
	if docMap, ok := q.Document.(map[string]interface{}); ok {
		args := make([]interface{}, 0, len(docMap)*2+1)
		args = append(args, key)
		for k, v := range docMap {
			args = append(args, k, v)
		}
		return &RedisCommand{
			Cmd:  "HMSET",
			Args: args,
		}, nil
	}

	// For simple values, use SET
	return &RedisCommand{
		Cmd:  "SET",
		Args: []interface{}{key, q.Document},
	}, nil
}

func (t *RedisTranslator) translateUpdate(q *query.Query) (*RedisCommand, error) {
	if idFilter := t.getIDFilter(q.Filters); idFilter != nil {
		key := t.buildKey(q.Collection, idFilter.Value)

		// For multiple field updates, use HMSET
		if len(q.Updates) > 1 {
			args := make([]interface{}, 0, len(q.Updates)*2+1)
			args = append(args, key)
			for k, v := range q.Updates {
				args = append(args, k, v)
			}
			return &RedisCommand{
				Cmd:  "HMSET",
				Args: args,
			}, nil
		}

		// For single field update, use HSET
		for k, v := range q.Updates {
			return &RedisCommand{
				Cmd:  "HSET",
				Args: []interface{}{key, k, v},
			}, nil
		}
	}

	return nil, fmt.Errorf("%w: update requires ID filter", ErrInvalidQuery)
}

func (t *RedisTranslator) translateDelete(q *query.Query) (*RedisCommand, error) {
	if idFilter := t.getIDFilter(q.Filters); idFilter != nil {
		key := t.buildKey(q.Collection, idFilter.Value)
		return &RedisCommand{
			Cmd:  "DEL",
			Args: []interface{}{key},
		}, nil
	}

	// For deleting by pattern, use SCAN + DEL
	pattern := t.buildKey(q.Collection, "*")
	return &RedisCommand{
		Cmd:  "EVAL", // Lua script for pattern-based delete
		Args: []interface{}{
			t.getDeleteScript(),
			0,
			pattern,
		},
	}, nil
}

func (t *RedisTranslator) translateCount(q *query.Query) (*RedisCommand, error) {
	if len(q.Filters) == 0 {
		// Count all keys in collection
		pattern := t.buildKey(q.Collection, "*")
		return &RedisCommand{
			Cmd:  "EVAL",
			Args: []interface{}{t.getCountScript(), 0, pattern},
		}, nil
	}

	return nil, fmt.Errorf("%w: count with filters not supported", ErrUnsupportedOperation)
}

// Helper methods

func (t *RedisTranslator) getIDFilter(filters []*query.Filter) *query.Filter {
	for _, filter := range filters {
		if filter.Field == "id" || filter.Field == "ID" {
			if len(filter.Nested) == 0 {
				return filter
			}
		}
		if len(filter.Nested) > 0 {
			if nested := t.getIDFilter(filter.Nested); nested != nil {
				return nested
			}
		}
	}
	return nil
}

func (t *RedisTranslator) buildKey(collection string, id interface{}) string {
	key := strings.ReplaceAll(t.keyPattern, "{collection}", collection)
	if id != nil {
		key = strings.ReplaceAll(key, "{id}", fmt.Sprintf("%v", id))
	}
	return key
}

func (t *RedisTranslator) generateKey(collection string, document interface{}) string {
	// Try to extract ID from document
	// TODO: Use reflection to get ID field
	return t.buildKey(collection, "auto")
}

// Lua scripts for complex operations

func (t *RedisTranslator) getDeleteScript() string {
	return `
local keys = redis.call('KEYS', ARGV[1])
if #keys > 0 then
	return redis.call('DEL', unpack(keys))
end
return 0
`
}

func (t *RedisTranslator) getCountScript() string {
	return `
local keys = redis.call('KEYS', ARGV[1])
return #keys
`
}
