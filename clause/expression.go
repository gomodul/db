package clause

// Expression expression interface
type Expression interface {
	Build(builder Builder)
}

// Expr raw expression
type Expr struct {
	SQL                string
	Vars               []interface{}
	WithoutParentheses bool
}

// Build implements the Expression interface
func (e Expr) Build(builder Builder) {
	builder.WriteString(e.SQL)
	if len(e.Vars) > 0 {
		builder.AddVar(builder, e.Vars...)
	}
}
