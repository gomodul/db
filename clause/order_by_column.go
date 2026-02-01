package clause

type OrderByColumn struct {
	Column  Column
	Desc    bool
	Reorder bool
}
