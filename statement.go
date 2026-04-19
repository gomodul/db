package db

import (
	"context"
	"reflect"
	"strings"
	"sync"

	"github.com/gomodul/db/clause"
	"github.com/gomodul/db/dialect"
	"github.com/gomodul/db/schema"
)

// Statement statement
type Statement struct {
	*DB
	TableExpr            *clause.Expr
	Table                string
	Model                interface{}
	Unscoped             bool
	Dest                 interface{}
	ReflectValue         reflect.Value
	Clauses              map[string]clause.Clause
	BuildClauses         []string
	Distinct             bool
	Selects              []string          // selected columns
	Omits                []string          // omit columns
	ColumnMapping        map[string]string // map columns
	Joins                []join
	Preloads             map[string][]interface{}
	Settings             sync.Map
	Schema               *schema.Schema
	Context              context.Context
	RaiseErrorOnNotFound bool
	SkipHooks            bool
	SQL                  strings.Builder
	Vars                 []interface{}
	CurDestIndex         int
	attrs                []interface{}
	assigns              []interface{}
	scopes               []func(*DB) *DB
	Result               *dialect.Result
}
