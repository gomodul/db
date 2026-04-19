package db

import "github.com/gomodul/db/migrate"

// Migrate is the schema migration handler returned by DB.Migrator().
// Use DB.Migrator() to obtain an instance.
type Migrate = migrate.Migrator

// IndexInfo is re-exported from the migrate package for convenience.
type IndexInfo = migrate.IndexInfo

// ColumnInfo is re-exported from the migrate package for convenience.
type ColumnInfo = migrate.ColumnInfo
