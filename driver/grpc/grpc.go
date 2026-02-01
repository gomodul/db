package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/query"
)

// Driver implements the dialect.Driver interface for gRPC services
type Driver struct {
	conn   *grpc.ClientConn
	config *dialect.Config
}

// NewDriver creates a new gRPC driver
func NewDriver() *Driver {
	return &Driver{}
}

// Name returns the driver name
func (d *Driver) Name() string {
	return "grpc"
}

// Type returns the driver type
func (d *Driver) Type() dialect.DriverType {
	return dialect.TypeRPC
}

// Initialize initializes the gRPC connection
func (d *Driver) Initialize(cfg *dialect.Config) error {
	d.config = cfg

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Parse DSN as target address
	target := cfg.DSN

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to grpc: %w", err)
	}

	d.conn = conn
	return nil
}

// Close closes the gRPC connection
func (d *Driver) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

// Execute executes a universal query via gRPC
func (d *Driver) Execute(ctx context.Context, q *query.Query) (*dialect.Result, error) {
	// gRPC requires service and method information
	service, method := d.getServiceAndMethod(q)

	// Build request message from query
	request, err := d.buildRequest(q)
	if err != nil {
		return nil, err
	}

	// Invoke gRPC method
	err = d.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", service, method), request, nil)
	if err != nil {
		return nil, fmt.Errorf("grpc invocation error: %w", err)
	}

	// Return empty result - actual response handling requires generated code
	return &dialect.Result{
		Data:        []interface{}{},
		RowsAffected: 0,
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
			},
		},
	}
}

// Ping checks if the gRPC service is reachable
func (d *Driver) Ping(ctx context.Context) error {
	state := d.conn.GetState()
	if state == connectivity.Shutdown {
		return fmt.Errorf("grpc connection is shutdown")
	}
	return nil
}

// Health returns the health status
func (d *Driver) Health() (*dialect.HealthStatus, error) {
	start := time.Now()

	state := d.conn.GetState()
	// Check if connection is in a ready or connecting state
	if state == connectivity.Shutdown || state == connectivity.TransientFailure {
		return dialect.NewUnhealthyStatus(fmt.Sprintf("grpc state: %s", state)), nil
	}

	return dialect.NewHealthyStatus(time.Since(start)).WithDetail("state", state.String()), nil
}

// BeginTx returns error - gRPC doesn't support traditional transactions
func (d *Driver) BeginTx(ctx context.Context) (dialect.Transaction, error) {
	return nil, dialect.ErrNotSupported
}

// Helper methods

func (d *Driver) getServiceAndMethod(q *query.Query) (service, method string) {
	// Try to get service/method from hints
	if q.Hints != nil {
		if s, ok := q.Hints["service"].(string); ok {
			service = s
		}
		if m, ok := q.Hints["method"].(string); ok {
			method = m
		}
	}

	// Default values
	if service == "" {
		service = "Service"
	}
	if method == "" {
		method = "Method"
	}

	return service, method
}

func (d *Driver) buildRequest(q *query.Query) (interface{}, error) {
	// Try to use document as request
	if q.Document != nil {
		return q.Document, nil
	}

	// Try to use updates as request
	if len(q.Updates) > 0 {
		return q.Updates, nil
	}

	// Build from filters
	if len(q.Filters) > 0 {
		request := make(map[string]interface{})
		for _, filter := range q.Filters {
			request[filter.Field] = filter.Value
		}
		return request, nil
	}

	return map[string]interface{}{}, nil
}

func init() {
	// Register the gRPC driver
	dialect.Register("grpc", func() dialect.Driver {
		return NewDriver()
	})
}
