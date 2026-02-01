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

// IsErrNotFound returns true if the error is ErrNotFound.
func IsErrNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsErrDuplicate returns true if the error is ErrDuplicate.
func IsErrDuplicate(err error) bool {
	return errors.Is(err, ErrDuplicate)
}

// IsErrClosed returns true if the error is ErrClosed.
func IsErrClosed(err error) bool {
	return errors.Is(err, ErrClosed)
}

// IsErrTxDone returns true if the error is ErrTxDone.
func IsErrTxDone(err error) bool {
	return errors.Is(err, ErrTxDone)
}

// IsErrNoDriver returns true if the error is ErrNoDriver.
func IsErrNoDriver(err error) bool {
	return errors.Is(err, ErrNoDriver)
}

// IsErrNotSupported returns true if the error is ErrNotSupported.
func IsErrNotSupported(err error) bool {
	return errors.Is(err, ErrNotSupported)
}
