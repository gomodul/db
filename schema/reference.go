package schema

type Reference struct {
	PrimaryKey    *Field
	PrimaryValue  string
	ForeignKey    *Field
	OwnPrimaryKey bool
}
