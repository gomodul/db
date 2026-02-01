package db

import "errors"

var (
	// ErrNotFound is returned when a record is not found.
	ErrNotFound = errors.New("db: not found")

	// ErrDuplicate is returned when a unique constraint is violated.
	ErrDuplicate = errors.New("db: duplicate entry")

	// ErrClosed is returned when operating on a closed connection.
	ErrClosed = errors.New("db: connection closed")

	// ErrTxDone is returned when a transaction has already been committed or rolled back.
	ErrTxDone = errors.New("db: transaction already done")

	// ErrNoDriver is returned when no driver is registered for the given engine.
	ErrNoDriver = errors.New("db: no driver registered")

	// ErrNotSupported is returned when an operation is not supported by the engine.
	ErrNotSupported = errors.New("db: operation not supported")
)
