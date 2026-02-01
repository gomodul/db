package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/gomodul/db/dialect"
)

// Migrator implements the dialect.Migrator interface for Elasticsearch
type Migrator struct {
	driver *Driver
}

// AutoMigrate creates indices based on models
func (m *Migrator) AutoMigrate(models ...interface{}) error {
	// TODO: Implement model introspection and index creation
	return nil
}

// CreateCollection creates a new index
func (m *Migrator) CreateCollection(name string, models ...interface{}) error {
	indexMapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{},
		},
	}

	// Build properties from models
	// TODO: Extract field definitions from models

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(indexMapping); err != nil {
		return err
	}

	req := esapi.IndicesCreateRequest{
		Index: name,
		Body:  &buf,
	}

	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch create index error: %s", res.Status())
	}

	return nil
}

// DropCollection drops an index
func (m *Migrator) DropCollection(name string) error {
	req := esapi.IndicesDeleteRequest{
		Index: []string{name},
	}

	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch delete index error: %s", res.Status())
	}

	return nil
}

// HasCollection checks if an index exists
func (m *Migrator) HasCollection(name string) bool {
	req := esapi.IndicesExistsRequest{
		Index: []string{name},
	}

	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return false
	}
	defer res.Body.Close()

	return res.StatusCode == 200
}

// RenameCollection renames an index
func (m *Migrator) RenameCollection(oldName, newName string) error {
	// Elasticsearch doesn't support renaming directly
	// Would require: create new index, reindex data, delete old index
	return fmt.Errorf("elasticsearch does not support renaming indices directly")
}

// CreateIndex creates an index with mapping
func (m *Migrator) CreateIndex(collection, name string, fields []string, unique bool) error {
	// Build index mapping
	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{},
		},
	}

	// TODO: Build field mappings from fields parameter

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(mapping); err != nil {
		return err
	}

	req := esapi.IndicesCreateRequest{
		Index: name,
		Body:  &buf,
	}

	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch create index error: %s", res.Status())
	}

	return nil
}

// DropIndex drops an index
func (m *Migrator) DropIndex(collection, name string) error {
	req := esapi.IndicesDeleteRequest{
		Index: []string{name},
	}

	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch delete index error: %s", res.Status())
	}

	return nil
}

// HasIndex checks if an index exists
func (m *Migrator) HasIndex(collection, name string) bool {
	return m.HasCollection(name)
}

// GetIndexes returns all indexes
func (m *Migrator) GetIndexes(collection string) ([]dialect.Index, error) {
	req := esapi.IndicesGetRequest{
		Index: []string{"*"},
	}

	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch get indexes error: %s", res.Status())
	}

	var response map[string]map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, err
	}

	var indexes []dialect.Index
	for name, indexDef := range response {
		indexes = append(indexes, dialect.Index{
			Name: name,
			Fields: extractFieldNames(indexDef),
		})
	}

	return indexes, nil
}

// AddColumn adds a field to the index mapping
func (m *Migrator) AddColumn(collection, field string) error {
	// Elasticsearch requires recreating the index to add fields
	return fmt.Errorf("elasticsearch does not support adding columns without recreating the index")
}

// DropColumn removes a field from the index mapping
func (m *Migrator) DropColumn(collection, field string) error {
	// Elasticsearch requires recreating the index to remove fields
	return fmt.Errorf("elasticsearch does not support dropping columns without recreating the index")
}

// AlterColumn alters a column definition
func (m *Migrator) AlterColumn(collection, field string) error {
	return fmt.Errorf("elasticsearch does not support altering columns without recreating the index")
}

func extractFieldNames(indexDef map[string]interface{}) []string {
	// TODO: Extract field names from index mapping
	return nil
}
