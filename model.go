package db

import "time"

// Model is a base struct that provides common fields for database models.
// Embed it in your own structs to get automatic ID, timestamp, and soft delete support.
//
//	type User struct {
//	    db.Model
//	    Name  string `db:"name,notnull"`
//	    Email string `db:"email,unique,notnull"`
//	}
type Model struct {
	ID        any       `db:"id,pk"`
	CreatedAt time.Time `db:"created_at,notnull"`
	UpdatedAt time.Time `db:"updated_at"`
	DeletedAt DeletedAt `db:"deleted_at,softdelete,index"`
}
