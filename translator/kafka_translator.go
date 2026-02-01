package translator

import (
	"encoding/json"
	"fmt"

	"github.com/gomodul/db/query"
)

// KafkaTranslator translates universal queries to Kafka messages
type KafkaTranslator struct {
	*BaseTranslator
	topicPrefix string // Prefix for topic names
}

// NewKafkaTranslator creates a new Kafka translator
func NewKafkaTranslator(topicPrefix string) *KafkaTranslator {
	if topicPrefix == "" {
		topicPrefix = "db"
	}
	return &KafkaTranslator{
		BaseTranslator: NewBaseTranslator("kafka"),
		topicPrefix:     topicPrefix,
	}
}

// KafkaMessage represents a translated Kafka message
type KafkaMessage struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Translate converts universal query to Kafka message
func (t *KafkaTranslator) Translate(q *query.Query) (BackendQuery, error) {
	if err := t.Validate(q); err != nil {
		return nil, err
	}

	topic := t.getTopic(q)
	key := t.buildKey(q)
	value, err := t.buildValue(q)
	if err != nil {
		return nil, err
	}

	return &KafkaMessage{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: t.buildHeaders(q),
	}, nil
}

func (t *KafkaTranslator) getTopic(q *query.Query) string {
	collection := t.getCollectionName(q)

	switch q.Operation {
	case query.OpCreate:
		return fmt.Sprintf("%s.%s.created", t.topicPrefix, collection)
	case query.OpUpdate:
		return fmt.Sprintf("%s.%s.updated", t.topicPrefix, collection)
	case query.OpDelete:
		return fmt.Sprintf("%s.%s.deleted", t.topicPrefix, collection)
	case query.OpFind:
		return fmt.Sprintf("%s.%s.query", t.topicPrefix, collection)
	case query.OpAggregate:
		return fmt.Sprintf("%s.%s.aggregate", t.topicPrefix, collection)
	default:
		return fmt.Sprintf("%s.%s", t.topicPrefix, collection)
	}
}

func (t *KafkaTranslator) buildKey(q *query.Query) []byte {
	// Extract ID from document or filters for message key
	var id interface{}

	if q.Document != nil {
		id = t.extractID(q.Document)
	}

	if id == nil && len(q.Filters) > 0 {
		for _, filter := range q.Filters {
			if filter.Field == "id" || filter.Field == "ID" {
				id = filter.Value
				break
			}
		}
	}

	if id != nil {
		return []byte(fmt.Sprintf("%v", id))
	}
	return []byte("unknown")
}

func (t *KafkaTranslator) buildValue(q *query.Query) ([]byte, error) {
	payload := make(map[string]interface{})

	payload["operation"] = string(q.Operation)
	payload["collection"] = t.getCollectionName(q)
	payload["timestamp"] = q.Context.Value("timestamp")

	switch q.Operation {
	case query.OpCreate:
		payload["data"] = q.Document

	case query.OpUpdate:
		payload["filter"] = t.buildKafkaFilter(q.Filters)
		payload["updates"] = q.Updates

	case query.OpDelete:
		payload["filter"] = t.buildKafkaFilter(q.Filters)

	case query.OpFind:
		payload["filter"] = t.buildKafkaFilter(q.Filters)
		payload["select"] = q.Selects
		payload["order"] = t.buildKafkaOrder(q.Orders)
		payload["limit"] = q.Limit
		payload["offset"] = q.Offset

	case query.OpCount:
		payload["filter"] = t.buildKafkaFilter(q.Filters)

	case query.OpAggregate:
		payload["filter"] = t.buildKafkaFilter(q.Filters)
		payload["groups"] = q.Groups
		payload["aggregates"] = q.Aggregates
	}

	return json.Marshal(payload)
}

func (t *KafkaTranslator) buildHeaders(q *query.Query) map[string]string {
	headers := make(map[string]string)

	headers["operation"] = string(q.Operation)
	headers["collection"] = t.getCollectionName(q)

	// Add correlation ID if present
	if q.TxID != "" {
		headers["tx_id"] = q.TxID
	}

	// Add content type
	headers["content_type"] = "application/json"

	return headers
}

func (t *KafkaTranslator) getCollectionName(q *query.Query) string {
	if q.Collection != "" {
		return q.Collection
	}
	// TODO: Extract from model using reflection
	return "resource"
}

func (t *KafkaTranslator) extractID(doc interface{}) interface{} {
	// Try to extract ID from document
	// TODO: Use reflection to get ID field
	if m, ok := doc.(map[string]interface{}); ok {
		if id, exists := m["id"]; exists {
			return id
		}
		if id, exists := m["ID"]; exists {
			return id
		}
		if id, exists := m["_id"]; exists {
			return id
		}
	}
	return nil
}

func (t *KafkaTranslator) buildKafkaFilter(filters []*query.Filter) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

	for _, filter := range filters {
		if len(filter.Nested) > 0 {
			nested := t.buildKafkaFilter(filter.Nested)
			result = append(result, map[string]interface{}{
				"logic":  filter.Logic,
				"nested": nested,
			})
		} else {
			result = append(result, map[string]interface{}{
				"field":    filter.Field,
				"operator": filter.Operator,
				"value":    filter.Value,
				"logic":    filter.Logic,
			})
		}
	}

	return result
}

func (t *KafkaTranslator) buildKafkaOrder(orders []*query.Order) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(orders))

	for _, order := range orders {
		result = append(result, map[string]interface{}{
			"field":     order.Field,
			"direction": order.Direction,
		})
	}

	return result
}
