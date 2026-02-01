package dialect

import "github.com/gomodul/db/query"

// Capabilities declares what a driver can do
type Capabilities struct {
	Query       QueryCapabilities
	Transaction TransactionCapabilities
	Schema      SchemaCapabilities
	Indexing    IndexCapabilities

	// Performance
	BatchSize int
	Streaming bool

	// Features
	Features map[string]bool
}

// QueryCapabilities represents query-related capabilities
type QueryCapabilities struct {
	// CRUD operations
	Create      bool
	Read        bool
	Update      bool
	Delete      bool
	BatchCreate bool
	BatchUpdate bool
	BatchDelete bool

	// Filtering
	Filters          []query.FilterOperator
	LogicalOperators []query.LogicOperator
	NestedFilters    bool

	// Sorting
	Sort           bool
	MultiFieldSort bool

	// Pagination
	OffsetPagination bool
	CursorPagination bool

	// Aggregation
	GroupBy      bool
	Aggregations []query.AggOperator

	// Relationships
	Joins     bool
	NestedJoins bool
	Preload    bool

	// Advanced
	Subqueries     bool
	Unions         bool
	Hints          bool
	Locking        bool
	FullTextSearch bool
	Geospatial     bool
}

// TransactionCapabilities represents transaction-related capabilities
type TransactionCapabilities struct {
	Supported       bool
	Nested          bool
	Savepoints      bool
	IsolationLevels []IsolationLevel
}

// IsolationLevel represents transaction isolation levels
type IsolationLevel string

const (
	LevelReadUncommitted IsolationLevel = "read_uncommitted"
	LevelReadCommitted   IsolationLevel = "read_committed"
	LevelRepeatableRead  IsolationLevel = "repeatable_read"
	LevelSerializable    IsolationLevel = "serializable"
	LevelSnapshot        IsolationLevel = "snapshot"
)

// SchemaCapabilities represents schema-related capabilities
type SchemaCapabilities struct {
	AutoMigrate       bool
	CreateTables      bool
	AlterTables       bool
	DropTables        bool
	CreateIndexes     bool
	DropIndexes       bool
	Constraints       bool
	ForeignKeys       bool
	CheckConstraints  bool
}

// IndexCapabilities represents index-related capabilities
type IndexCapabilities struct {
	Unique    bool
	Composite bool
	Partial   bool
	FullText  bool
	Geospatial bool
	Hash      bool
	BTree     bool
	GiST      bool
}

// HasFilter returns true if the driver supports the given filter operator
func (c *QueryCapabilities) HasFilter(op query.FilterOperator) bool {
	for _, supported := range c.Filters {
		if supported == op {
			return true
		}
	}
	return false
}

// HasAggregation returns true if the driver supports the given aggregation operator
func (c *QueryCapabilities) HasAggregation(op query.AggOperator) bool {
	for _, supported := range c.Aggregations {
		if supported == op {
			return true
		}
	}
	return false
}

// HasIsolationLevel returns true if the driver supports the given isolation level
func (c *TransactionCapabilities) HasIsolationLevel(level IsolationLevel) bool {
	for _, supported := range c.IsolationLevels {
		if supported == level {
			return true
		}
	}
	return false
}

// HasFeature returns true if the driver supports the given feature
func (c *Capabilities) HasFeature(feature string) bool {
	if c.Features == nil {
		return false
	}
	v, ok := c.Features[feature]
	return ok && v
}

// SetFeature sets a feature capability
func (c *Capabilities) SetFeature(feature string, enabled bool) {
	if c.Features == nil {
		c.Features = make(map[string]bool)
	}
	c.Features[feature] = enabled
}
