package db

// BeforeCreate is an interface that can be implemented by models to run code before creation.
type BeforeCreate interface {
	BeforeCreate(*DB) error
}

// AfterCreate is an interface that can be implemented by models to run code after creation.
type AfterCreate interface {
	AfterCreate(*DB) error
}

// BeforeUpdate is an interface that can be implemented by models to run code before update.
type BeforeUpdate interface {
	BeforeUpdate(*DB) error
}

// AfterUpdate is an interface that can be implemented by models to run code after update.
type AfterUpdate interface {
	AfterUpdate(*DB) error
}

// BeforeDelete is an interface that can be implemented by models to run code before deletion.
type BeforeDelete interface {
	BeforeDelete(*DB) error
}

// AfterDelete is an interface that can be implemented by models to run code after deletion.
type AfterDelete interface {
	AfterDelete(*DB) error
}

// BeforeFind is an interface that can be implemented by models to run code before query.
type BeforeFind interface {
	BeforeFind(*DB) error
}

// AfterFind is an interface that can be implemented by models to run code after query.
type AfterFind interface {
	AfterFind(*DB) error
}
