package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/gomodul/db/dialect"
)

// Migrator implements the dialect.Migrator interface for MongoDB
type Migrator struct {
	driver *Driver
}

func (m *Migrator) AutoMigrate(models ...interface{}) error {
	return nil
}

func (m *Migrator) CreateTable(name string, models ...interface{}) error {
	ctx := context.Background()
	err := m.driver.database.CreateCollection(ctx, name)
	if err != nil {
		return fmt.Errorf("mongodb: create collection %s: %w", name, err)
	}
	return nil
}

func (m *Migrator) DropTable(name string) error {
	ctx := context.Background()
	err := m.driver.database.Collection(name).Drop(ctx)
	if err != nil {
		return fmt.Errorf("mongodb: drop collection %s: %w", name, err)
	}
	return nil
}

func (m *Migrator) HasTable(name string) bool {
	ctx := context.Background()
	cols, err := m.driver.database.ListCollectionNames(ctx, bson.M{"name": name})
	if err != nil {
		return false
	}
	return len(cols) > 0
}

func (m *Migrator) RenameTable(oldName, newName string) error {
	if !m.HasTable(oldName) {
		return fmt.Errorf("mongodb: collection %s does not exist", oldName)
	}
	if m.HasTable(newName) {
		return fmt.Errorf("mongodb: collection %s already exists", newName)
	}

	ctx := context.Background()
	oldColl := m.driver.database.Collection(oldName)
	newColl := m.driver.database.Collection(newName)

	cursor, err := oldColl.Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	var docs []interface{}
	if err := cursor.All(ctx, &docs); err != nil {
		return err
	}
	if len(docs) > 0 {
		if _, err = newColl.InsertMany(ctx, docs); err != nil {
			return err
		}
	}
	return m.DropTable(oldName)
}

func (m *Migrator) GetTables() ([]string, error) {
	ctx := context.Background()
	return m.driver.database.ListCollectionNames(ctx, bson.M{})
}

// MongoDB is schemaless — column operations are no-ops or best-effort.

func (m *Migrator) AddColumn(table, column, columnType string) error {
	return nil
}

func (m *Migrator) DropColumn(table, column string) error {
	ctx := context.Background()
	_, err := m.driver.database.Collection(table).UpdateMany(
		ctx, bson.M{}, bson.M{"$unset": bson.M{column: ""}},
	)
	return err
}

func (m *Migrator) AlterColumn(table, column, newType string) error {
	return nil
}

func (m *Migrator) HasColumn(table, column string) bool {
	return false
}

func (m *Migrator) RenameColumn(table, oldName, newName string) error {
	ctx := context.Background()
	_, err := m.driver.database.Collection(table).UpdateMany(
		ctx, bson.M{oldName: bson.M{"$exists": true}},
		bson.M{"$rename": bson.M{oldName: newName}},
	)
	return err
}

func (m *Migrator) CreateIndex(table, name string, fields []string, unique bool) error {
	ctx := context.Background()
	keys := bson.D{}
	for _, f := range fields {
		keys = append(keys, bson.E{Key: f, Value: 1})
	}
	model := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetName(name).SetUnique(unique),
	}
	_, err := m.driver.database.Collection(table).Indexes().CreateOne(ctx, model)
	if err != nil {
		return fmt.Errorf("mongodb: create index %s: %w", name, err)
	}
	return nil
}

func (m *Migrator) DropIndex(table, name string) error {
	ctx := context.Background()
	_, err := m.driver.database.Collection(table).Indexes().DropOne(ctx, name)
	if err != nil {
		return fmt.Errorf("mongodb: drop index %s: %w", name, err)
	}
	return nil
}

func (m *Migrator) HasIndex(table, name string) bool {
	ctx := context.Background()
	cursor, err := m.driver.database.Collection(table).Indexes().List(ctx)
	if err != nil {
		return false
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var idx bson.M
		if err := cursor.Decode(&idx); err != nil {
			continue
		}
		if n, ok := idx["name"].(string); ok && n == name {
			return true
		}
	}
	return false
}

func (m *Migrator) GetIndexes(table string) ([]dialect.Index, error) {
	ctx := context.Background()
	cursor, err := m.driver.database.Collection(table).Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("mongodb: list indexes for %s: %w", table, err)
	}
	defer cursor.Close(ctx)

	var indexes []dialect.Index
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		idx := dialect.Index{Name: doc["name"].(string)}
		if u, ok := doc["unique"].(bool); ok {
			idx.Unique = u
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}
