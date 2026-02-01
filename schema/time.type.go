package schema

type (
	// TimeType GORM time type
	TimeType int64
)

const (
	UnixTime        TimeType = 1
	UnixSecond      TimeType = 2
	UnixMillisecond TimeType = 3
	UnixNanosecond  TimeType = 4
)
