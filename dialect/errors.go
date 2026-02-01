package dialect

import "errors"

var (
	// ErrTxDone is returned when trying to operate on a completed transaction
	ErrTxDone = errors.New("driver: transaction already done")

	// ErrNotSupported is returned when an operation is not supported by the driver
	ErrNotSupported = errors.New("driver: operation not supported")

	// ErrNoDriver is returned when no driver is found for the given DSN
	ErrNoDriver = errors.New("driver: no driver registered")

	// ErrInvalidConfig is returned when the driver config is invalid
	ErrInvalidConfig = errors.New("driver: invalid configuration")

	// ErrConnectionFailed is returned when connection fails
	ErrConnectionFailed = errors.New("driver: connection failed")

	// ErrQueryFailed is returned when a query fails
	ErrQueryFailed = errors.New("driver: query failed")

	// ErrTransactionFailed is returned when a transaction fails
	ErrTransactionFailed = errors.New("driver: transaction failed")

	// ErrNotFound is returned when a record is not found
	ErrNotFound = errors.New("driver: record not found")

	// ErrDuplicate is returned when a duplicate key error occurs
	ErrDuplicate = errors.New("driver: duplicate key")
)
