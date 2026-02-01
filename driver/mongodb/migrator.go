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

// AutoMigrate is a no-op for MongoDB (schemaless)
func (m *Migrator) AutoMigrate(models ...interface{}) error {
	// MongoDB is schemaless, no migration needed
	return nil
}

// CreateCollection creates a new collection
func (m *Migrator) CreateCollection(name string, models ...interface{}) error {
	ctx := context.Background()
	err := m.driver.database.CreateCollection(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}
	return nil
}

// DropCollection drops a collection
func (m *Migrator) DropCollection(name string) error {
	ctx := context.Background()
	err := m.driver.database.Collection(name).Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to drop collection: %w", err)
	}
	return nil
}

// HasCollection checks if a collection exists
func (m *Migrator) HasCollection(name string) bool {
	ctx := context.Background()
	collections, err := m.driver.database.ListCollectionNames(ctx, bson.M{"name": name})
	if err != nil {
		return false
	}
	return len(collections) > 0
}

// RenameCollection renames a collection
func (m *Migrator) RenameCollection(oldName, newName string) error {
	// MongoDB doesn't have a direct rename command, need to copy and delete
	ctx := context.Background()

	// Check if old collection exists
	if !m.HasCollection(oldName) {
		return fmt.Errorf("collection %s does not exist", oldName)
	}

	// Check if new collection already exists
	if m.HasCollection(newName) {
		return fmt.Errorf("collection %s already exists", newName)
	}

	// Copy documents from old to new collection
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
		_, err = newColl.InsertMany(ctx, docs)
		if err != nil {
			return err
		}
	}

	// Drop old collection
	return m.DropCollection(oldName)
}

// CreateIndex creates an index
func (m *Migrator) CreateIndex(collection, name string, fields []string, unique bool) error {
	ctx := context.Background()

	keys := bson.D{}
	for _, field := range fields {
		keys = append(keys, bson.E{Key: field, Value: 1})
	}

	index := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetName(name).SetUnique(unique),
	}

	_, err := m.driver.database.Collection(collection).Indexes().CreateOne(ctx, index)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	return nil
}

// DropIndex drops an index
func (m *Migrator) DropIndex(collection, name string) error {
	ctx := context.Background()
	_, err := m.driver.database.Collection(collection).Indexes().DropOne(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to drop index: %w", err)
	}
	return nil
}

// HasIndex checks if an index exists
func (m *Migrator) HasIndex(collection, name string) bool {
	ctx := context.Background()

	cursor, err := m.driver.database.Collection(collection).Indexes().List(ctx)
	if err != nil {
		return false
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			continue
		}
		if indexName, ok := index["name"].(string); ok && indexName == name {
			return true
		}
	}

	return false
}

// GetIndexes returns all indexes for a collection
func (m *Migrator) GetIndexes(collection string) ([]dialect.Index, error) {
	ctx := context.Background()

	cursor, err := m.driver.database.Collection(collection).Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes: %w", err)
	}
	defer cursor.Close(ctx)

	var indexes []dialect.Index
	for cursor.Next(ctx) {
		var indexDoc bson.M
		if err := cursor.Decode(&indexDoc); err != nil {
			continue
		}

		index := dialect.Index{
			Name: indexDoc["name"].(string),
		}

		if unique, ok := indexDoc["unique"].(bool); ok {
			index.Unique = unique
		}

		indexes = append(indexes, index)
	}

	return indexes, nil
}

// AddColumn is a no-op for MongoDB (schemaless)
func (m *Migrator) AddColumn(collection, field string) error {
	// MongoDB is schemaless, columns don't need to be explicitly added
	return nil
}

// DropColumn is a no-op for MongoDB (schemaless)
func (m *Migrator) DropColumn(collection, field string) error {
	// MongoDB is schemaless, but we could unset the field from all documents
	ctx := context.Background()
	_, err := m.driver.database.Collection(collection).UpdateMany(
		ctx,
		bson.M{},
		bson.M{"$unset": bson.M{field: ""}},
	)
	return err
}

// AlterColumn is a no-op for MongoDB (schemaless)
func (m *Migrator) AlterColumn(collection, field string) error {
	// MongoDB is schemaless, no column types to alter
	return nil
}
