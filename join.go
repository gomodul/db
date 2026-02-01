package db

import "github.com/gomodul/db/clause"

type join struct {
	Name       string
	Alias      string
	Conds      []interface{}
	On         *clause.Where
	Selects    []string
	Omits      []string
	Expression clause.Expression
	JoinType   clause.JoinType
}
