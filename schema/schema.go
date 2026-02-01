package schema

import (
	"reflect"
	"sync"

	"github.com/gomodul/db/clause"
)

type Schema struct {
	Name                      string
	ModelType                 reflect.Type
	Table                     string
	PrioritizedPrimaryField   *Field
	DBNames                   []string
	PrimaryFields             []*Field
	PrimaryFieldDBNames       []string
	Fields                    []*Field
	FieldsByName              map[string]*Field
	FieldsByBindName          map[string]*Field // embedded fields is 'Embed.Field'
	FieldsByDBName            map[string]*Field
	FieldsWithDefaultDBValue  []*Field // fields with default value assigned by database
	Relationships             Relationships
	CreateClauses             []clause.Interface
	QueryClauses              []clause.Interface
	UpdateClauses             []clause.Interface
	DeleteClauses             []clause.Interface
	BeforeCreate, AfterCreate bool
	BeforeUpdate, AfterUpdate bool
	BeforeDelete, AfterDelete bool
	BeforeSave, AfterSave     bool
	AfterFind                 bool
	err                       error
	initialized               chan struct{}
	name                      Name
	cacheStore                *sync.Map
}
