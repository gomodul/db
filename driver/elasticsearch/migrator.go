package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/gomodul/db/dialect"
)

// Migrator implements the dialect.Migrator interface for Elasticsearch.
// In Elasticsearch, "tables" map to indices and "columns" map to mapping fields.
type Migrator struct {
	driver *Driver
}

func (m *Migrator) AutoMigrate(models ...interface{}) error { return nil }

func (m *Migrator) CreateTable(name string, models ...interface{}) error {
	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{},
		},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(mapping); err != nil {
		return err
	}
	req := esapi.IndicesCreateRequest{Index: name, Body: &buf}
	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch: create index %s: %s", name, res.Status())
	}
	return nil
}

func (m *Migrator) DropTable(name string) error {
	req := esapi.IndicesDeleteRequest{Index: []string{name}}
	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch: delete index %s: %s", name, res.Status())
	}
	return nil
}

func (m *Migrator) HasTable(name string) bool {
	req := esapi.IndicesExistsRequest{Index: []string{name}}
	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.StatusCode == 200
}

func (m *Migrator) RenameTable(oldName, newName string) error {
	return fmt.Errorf("elasticsearch: renaming indices requires reindex + delete")
}

func (m *Migrator) GetTables() ([]string, error) {
	req := esapi.IndicesGetRequest{Index: []string{"*"}}
	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch: list indices: %s", res.Status())
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(resp))
	for name := range resp {
		names = append(names, name)
	}
	return names, nil
}

func (m *Migrator) AddColumn(table, column, columnType string) error {
	return fmt.Errorf("elasticsearch: adding fields requires recreating the index")
}

func (m *Migrator) DropColumn(table, column string) error {
	return fmt.Errorf("elasticsearch: dropping fields requires recreating the index")
}

func (m *Migrator) AlterColumn(table, column, newType string) error {
	return fmt.Errorf("elasticsearch: altering field types requires recreating the index")
}

func (m *Migrator) HasColumn(table, column string) bool { return false }

func (m *Migrator) RenameColumn(table, oldName, newName string) error {
	return fmt.Errorf("elasticsearch: renaming fields requires recreating the index")
}

func (m *Migrator) CreateIndex(table, name string, fields []string, unique bool) error {
	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{},
		},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(mapping); err != nil {
		return err
	}
	req := esapi.IndicesCreateRequest{Index: name, Body: &buf}
	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch: create index %s: %s", name, res.Status())
	}
	return nil
}

func (m *Migrator) DropIndex(table, name string) error {
	req := esapi.IndicesDeleteRequest{Index: []string{name}}
	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("elasticsearch: drop index %s: %s", name, res.Status())
	}
	return nil
}

func (m *Migrator) HasIndex(table, name string) bool {
	return m.HasTable(name)
}

func (m *Migrator) GetIndexes(table string) ([]dialect.Index, error) {
	req := esapi.IndicesGetRequest{Index: []string{"*"}}
	res, err := req.Do(context.Background(), m.driver.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch: get indexes: %s", res.Status())
	}
	var resp map[string]map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return nil, err
	}
	var indexes []dialect.Index
	for name := range resp {
		indexes = append(indexes, dialect.Index{Name: name})
	}
	return indexes, nil
}
