package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// Driver implements the dialect.Driver interface for Kafka
type Driver struct {
	conn   *kafka.Conn
	config *dialect.Config
}

// NewDriver creates a new Kafka driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "kafka"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeMessageQueue
}

// Initialize initializes the Kafka connection
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Parse DSN as broker address
	conn, err := kafka.DialContext(ctx, "tcp", cfg.DSN)
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}

	d.conn = conn
	return nil
}

// Close closes the Kafka connection
func (d *Driver) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// Execute executes a universal query via Kafka
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	topic := d.getTopic(q)

	switch q.Operation {
	case query.OpCreate:
		return d.executeProduce(ctx, topic, q)
	case query.OpFind:
		return d.executeConsume(ctx, topic, q)
	case query.OpDelete:
		// Kafka doesn't support delete
		return nil, dialect.ErrNotSupported
	default:
		return nil, fmt.Errorf("unsupported operation: %s", q.Operation)
	}
}

// executeProduce sends a message to Kafka
func (d *Driver) executeProduce(ctx context.Context, topic string, q *query.Query) (*dialect.Result, error) {
	message, key, err := d.buildMessage(q)
	if err != nil {
		return nil, err
	}

	err = d.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return nil, err
	}

	// Write message to Kafka
	_, err = d.conn.WriteMessages(kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(message),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to write message: %w", err)
	}

	return &dialect.Result{
		RowsAffected: 1,
	}, nil
}

// executeConsume reads messages from Kafka
func (d *Driver) executeConsume(ctx context.Context, topic string, q *query.Query) (*dialect.Result, error) {
	partition := 0

	// Create reader for the topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{d.config.DSN},
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer reader.Close()

	// Set offset if specified
	if q.Offset != nil {
		err := reader.SetOffset(int64(*q.Offset))
		if err != nil {
			return nil, err
		}
	}

	// Read messages
	var messages []interface{}
	maxMessages := 10
	if q.Limit != nil {
		maxMessages = *q.Limit
	}

	for i := 0; i < maxMessages; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			break
		}

		messages = append(messages, map[string]interface{}{
			"key":       string(msg.Key),
			"value":     string(msg.Value),
			"offset":    msg.Offset,
			"partition": msg.Partition,
			"time":      msg.Time,
		})
	}

	return &dialect.Result{
		Data:        messages,
		RowsAffected: int64(len(messages)),
	}, nil
}

// Capabilities returns the driver's capabilities
func (d *Driver) Capabilities() *dialect.Capabilities {
	return &dialect.Capabilities{
		Query: dialect.QueryCapabilities{
			Create: true,
			Read:   true,
			Filters: []query.FilterOperator{
				query.OpEqual,
			},
		},
	}
}

// Ping checks if Kafka is reachable
func (d *Driver) Ping(ctx context.Context) error {
	// Try to fetch controller to verify connection
	_, err := d.conn.Controller()
	return err
}

// Health returns the health status
func (d *Driver) Health() (*dialect.HealthStatus, error) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := d.Ping(ctx); err != nil {
		return dialect.NewUnhealthyStatus(err.Error()), nil
	}

	// Get broker info
	partitions, err := d.conn.ReadPartitions()
	if err != nil {
		return dialect.NewHealthyStatus(time.Since(start)), nil
	}

	return dialect.NewHealthyStatus(time.Since(start)).WithDetail("partitions", len(partitions)), nil
}

// BeginTx returns error - Kafka doesn't support transactions in this context
func (d *Driver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	return nil, dialect.ErrNotSupported
}

// Helper methods

func (d *Driver) getTopic(q *query.Query) string {
	// Use collection as topic name
	if q.Collection != "" {
		return q.Collection
	}
	return "default"
}

func (d *Driver) buildMessage(q *query.Query) (message string, key string, err error) {
	// Try to get key from hints
	if q.Hints != nil {
		if k, ok := q.Hints["key"].(string); ok {
			key = k
		}
	}

	// Try to get key from filters
	for _, filter := range q.Filters {
		if filter.Field == "key" || filter.Field == "id" {
			key = fmt.Sprintf("%v", filter.Value)
			break
		}
	}

	// Get message value
	if q.Document != nil {
		if m, ok := q.Document.(map[string]interface{}); ok {
			if val, exists := m["value"]; exists {
				message = fmt.Sprintf("%v", val)
			} else {
				message = fmt.Sprintf("%v", m)
			}
		} else {
			message = fmt.Sprintf("%v", q.Document)
		}
	} else if len(q.Updates) > 0 {
		for _, v := range q.Updates {
			message = fmt.Sprintf("%v", v)
			break
		}
	} else {
		message = "{}"
	}

	return message, key, nil
}

func init() {
	// Register the Kafka driver
	dialect.Register("kafka", func() dialect.Driver {
		return NewDriver()
	})
}
