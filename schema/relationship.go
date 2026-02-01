package schema

import "sync"

type Relationship struct {
	Name                     string
	Type                     RelationshipType
	Field                    *Field
	Polymorphic              *Polymorphic
	References               []*Reference
	Schema                   *Schema
	FieldSchema              *Schema
	JoinTable                *Schema
	foreignKeys, primaryKeys []string
}

type Relationships struct {
	HasOne    []*Relationship
	BelongsTo []*Relationship
	HasMany   []*Relationship
	Many2Many []*Relationship
	Relations map[string]*Relationship

	EmbeddedRelations map[string]*Relationships

	Mux sync.RWMutex
}
