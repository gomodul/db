package dialect

// Feature represents database capabilities/features using bit flags
type Feature uint64

const (
	// Core CRUD features
	FeatureCreate Feature = 1 << iota
	FeatureRead
	FeatureUpdate
	FeatureDelete
	FeatureBatchCreate
	FeatureBatchUpdate
	FeatureBatchDelete

	// Query features
	FeatureFilters
	FeatureNestedFilters
	FeatureSort
	FeatureMultiFieldSort
	FeatureOffsetPagination
	FeatureCursorPagination
	FeatureGroupBy
	FeatureAggregations
	FeatureJoins
	FeatureNestedJoins
	FeatureSubqueries
	FeatureUnions
	FeatureHints
	FeatureLocking
	FeatureFullTextSearch
	FeatureGeospatial

	// Transaction features
	FeatureTransactions
	FeatureNestedTransactions
	FeatureSavepoints

	// Schema features
	FeatureAutoMigrate
	FeatureCreateTables
	FeatureAlterTables
	FeatureDropTables
	FeatureCreateIndexes
	FeatureDropIndexes
	FeatureConstraints
	FeatureForeignKeys
	FeatureCheckConstraints

	// Index features
	FeatureUniqueIndex
	FeatureCompositeIndex
	FeaturePartialIndex
	FeatureFullTextIndex
	FeatureGeospatialIndex
	FeatureHashIndex
	FeatureBTreeIndex
	FeatureGiSTIndex

	// Performance features
	FeatureStreaming
	FeaturePreparedStatements
	FeatureConnectionPooling

	// Advanced features
	FeatureWindowFunctions
	FeatureCTE
	FeatureFullOuterJoin
	FeatureReturningClause
	FeatureUpsert
	FeatureIgnoreConflict
	FeatureFilteredAggregates
)

// Has checks if the feature flag is set
func (f Feature) Has(feature Feature) bool {
	return f&feature != 0
}

// Set sets a feature flag
func (f *Feature) Set(feature Feature) {
	*f |= feature
}

// Clear clears a feature flag
func (f *Feature) Clear(feature Feature) {
	*f &= ^feature
}

// Toggle toggles a feature flag
func (f *Feature) Toggle(feature Feature) {
	*f ^= feature
}

// Common feature sets

// SQLFeatures represents common SQL database features
var SQLFeatures = Feature(
	FeatureCreate | FeatureRead | FeatureUpdate | FeatureDelete |
		FeatureBatchCreate | FeatureBatchUpdate | FeatureBatchDelete |
		FeatureFilters | FeatureNestedFilters |
		FeatureSort | FeatureMultiFieldSort |
		FeatureOffsetPagination |
		FeatureGroupBy | FeatureAggregations |
		FeatureJoins | FeatureNestedJoins |
		FeatureSubqueries | FeatureUnions |
		FeatureTransactions | FeatureSavepoints |
		FeatureAutoMigrate | FeatureCreateTables | FeatureAlterTables |
		FeatureCreateIndexes | FeatureDropIndexes |
		FeatureConstraints | FeatureForeignKeys |
		FeatureWindowFunctions | FeatureCTE |
		FeaturePreparedStatements | FeatureConnectionPooling,
)

// NoSQLFeatures represents common NoSQL database features
var NoSQLFeatures = Feature(
	FeatureCreate | FeatureRead | FeatureUpdate | FeatureDelete |
		FeatureBatchCreate | FeatureBatchUpdate | FeatureBatchDelete |
		FeatureFilters | FeatureNestedFilters |
		FeatureSort | FeatureMultiFieldSort |
		FeatureOffsetPagination |
		FeatureGroupBy | FeatureAggregations |
		FeatureJoins, // Via $lookup in MongoDB
)

// KVStoreFeatures represents key-value store features
var KVStoreFeatures = Feature(
	FeatureCreate | FeatureRead | FeatureUpdate | FeatureDelete |
		FeatureFilters, // Limited
)

// APIFeatures represents REST API features
var APIFeatures = Feature(
	FeatureCreate | FeatureRead | FeatureUpdate | FeatureDelete |
		FeatureOffsetPagination,
)

// Feature helpers

// CanCreate checks if create operation is supported
func (f Feature) CanCreate() bool {
	return f.Has(FeatureCreate)
}

// CanRead checks if read operation is supported
func (f Feature) CanRead() bool {
	return f.Has(FeatureRead)
}

// CanUpdate checks if update operation is supported
func (f Feature) CanUpdate() bool {
	return f.Has(FeatureUpdate)
}

// CanDelete checks if delete operation is supported
func (f Feature) CanDelete() bool {
	return f.Has(FeatureDelete)
}

// CanTransact checks if transactions are supported
func (f Feature) CanTransact() bool {
	return f.Has(FeatureTransactions)
}

// CanMigrate checks if schema migration is supported
func (f Feature) CanMigrate() bool {
	return f.Has(FeatureAutoMigrate)
}

// CanJoin checks if joins are supported
func (f Feature) CanJoin() bool {
	return f.Has(FeatureJoins)
}

// CanFilter checks if filtering is supported
func (f Feature) CanFilter() bool {
	return f.Has(FeatureFilters)
}

// CanPaginate checks if pagination is supported
func (f Feature) CanPaginate() bool {
	return f.Has(FeatureOffsetPagination)
}

// CanAggregate checks if aggregation is supported
func (f Feature) CanAggregate() bool {
	return f.Has(FeatureAggregations)
}

// CanSavepoint checks if savepoints are supported
func (f Feature) CanSavepoint() bool {
	return f.Has(FeatureSavepoints)
}
