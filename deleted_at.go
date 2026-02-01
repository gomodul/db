package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// DeletedAt represents a nullable timestamp used for soft deletes.
// A zero-value DeletedAt means the record has not been deleted.
type DeletedAt struct {
	Time  time.Time
	Valid bool
}

// NewDeletedAt creates a valid DeletedAt with the given time.
func NewDeletedAt(t time.Time) DeletedAt {
	return DeletedAt{Time: t, Valid: true}
}

// IsDeleted reports whether the record has been soft-deleted.
func (d DeletedAt) IsDeleted() bool {
	return d.Valid
}

// Scan implements the sql.Scanner interface.
func (d *DeletedAt) Scan(src any) error {
	if src == nil {
		d.Time, d.Valid = time.Time{}, false
		return nil
	}

	switch v := src.(type) {
	case time.Time:
		d.Time, d.Valid = v, true
		return nil
	default:
		return fmt.Errorf("db: cannot scan %T into DeletedAt", src)
	}
}

// Value implements the driver.Valuer interface.
func (d DeletedAt) Value() (driver.Value, error) {
	if !d.Valid {
		return nil, nil
	}
	return d.Time, nil
}

// MarshalJSON implements the json.Marshaler interface.
// Returns null if not deleted, or the timestamp if deleted.
func (d DeletedAt) MarshalJSON() ([]byte, error) {
	if !d.Valid {
		return json.Marshal(nil)
	}
	return json.Marshal(d.Time)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (d *DeletedAt) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		d.Time, d.Valid = time.Time{}, false
		return nil
	}

	var t time.Time
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}

	d.Time, d.Valid = t, true
	return nil
}
