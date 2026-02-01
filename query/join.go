package query

// Join represents a join relationship
type Join struct {
	Collection  string      // Table/collection to join
	Type        JoinType    // Type of join
	Conditions  []*Filter   // Join conditions
	Alias       string      // Alias for the joined collection
	ForeignKeys []string    // Foreign keys
	References  []string    // Referenced columns
}

// JoinType represents the type of join
type JoinType string

const (
	JoinInner      JoinType = "INNER"
	JoinLeft       JoinType = "LEFT"
	JoinRight      JoinType = "RIGHT"
	JoinCross      JoinType = "CROSS"
	JoinFull       JoinType = "FULL"
	JoinLookup     JoinType = "LOOKUP"  // For MongoDB $lookup
	JoinEmbed      JoinType = "EMBED"   // For embedded documents
)

// Clone creates a deep copy of the join
func (j *Join) Clone() *Join {
	newJ := &Join{
		Collection:  j.Collection,
		Type:        j.Type,
		Alias:       j.Alias,
		ForeignKeys: make([]string, len(j.ForeignKeys)),
		References:  make([]string, len(j.References)),
	}

	copy(newJ.ForeignKeys, j.ForeignKeys)
	copy(newJ.References, j.References)

	if j.Conditions != nil {
		newJ.Conditions = make([]*Filter, len(j.Conditions))
		for i, c := range j.Conditions {
			newJ.Conditions[i] = c.Clone()
		}
	}

	return newJ
}

// InnerJoin creates an INNER JOIN
func InnerJoin(collection string, conditions ...*Filter) *Join {
	return &Join{
		Collection: collection,
		Type:       JoinInner,
		Conditions: conditions,
	}
}

// LeftJoin creates a LEFT JOIN
func LeftJoin(collection string, conditions ...*Filter) *Join {
	return &Join{
		Collection: collection,
		Type:       JoinLeft,
		Conditions: conditions,
	}
}

// RightJoin creates a RIGHT JOIN
func RightJoin(collection string, conditions ...*Filter) *Join {
	return &Join{
		Collection: collection,
		Type:       JoinRight,
		Conditions: conditions,
	}
}

// Lookup creates a MongoDB-style $lookup
func Lookup(collection string, foreignField, localField, as string) *Join {
	return &Join{
		Collection: collection,
		Type:       JoinLookup,
		Conditions: []*Filter{
			Eq(foreignField, "$"+localField),
		},
		Alias: as,
	}
}
