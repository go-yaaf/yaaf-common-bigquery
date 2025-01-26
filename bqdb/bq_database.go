package bqdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/go-yaaf/yaaf-common/config"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"google.golang.org/api/iterator"
)

// BqDatabase struct implementing IDatabase for BigQuery
type BqDatabase struct {
	client *bigquery.Client
	projectId,
	dataSet string
}

// NewBqDatabase creates a new BigQuery database connection using a URI.
//
// The URI must follow the format: "bq://projectId:datasetName"
//   - The schema must be "bq".
//   - If the schema is invalid, it returns an error.
//   - If the URI is valid, it parses the project ID and dataset name, creates a BigQuery client,
//     and initializes a BqDatabase struct.
//
// Parameters:
// - uri: A string representing the BigQuery connection URI.
//
// Returns:
// - A new instance of the BqDatabase struct implementing the database.IDatabase interface.
// - An error if the URI format is invalid or the BigQuery client creation fails.
func NewBqDatabase(uri string) (database.IDatabase, error) {
	// Validate URI schema
	const schemaPrefix = "bq://"
	if !strings.HasPrefix(uri, schemaPrefix) {
		return nil, errors.New("invalid URI: schema must be 'bq'")
	}

	// Extract project ID and dataset name
	trimmedURI := strings.TrimPrefix(uri, schemaPrefix)
	parts := strings.Split(trimmedURI, ":")
	if len(parts) != 2 {
		return nil, errors.New("invalid URI format: must be 'bq://projectId:datasetName'")
	}

	projectId := parts[0]
	dataSet := parts[1]

	// Validate extracted values
	if projectId == "" || dataSet == "" {
		return nil, errors.New("invalid URI: projectId or datasetName is empty")
	}

	// Create BigQuery client
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	// Return BqDatabase instance
	return &BqDatabase{client: client, dataSet: dataSet, projectId: projectId}, nil
}

// Close Closer includes method Close()
func (db *BqDatabase) Close() error {
	return db.client.Close()
}

// Ping tests BigQuery connectivity (not really needed for BigQuery, stub)
func (db *BqDatabase) Ping(retries uint, intervalInSeconds uint) error {
	return nil
}

// CloneDatabase returns a clone (copy) of the database instance
func (db *BqDatabase) CloneDatabase() (database.IDatabase, error) {
	return &BqDatabase{client: db.client, dataSet: db.dataSet}, nil
}

// CRUD Operations (No-op implementations) --------------------------------------

// Get fetches a single entity by ID (not supported in BigQuery)
func (db *BqDatabase) Get(factory entity.EntityFactory, entityID string, keys ...string) (entity.Entity, error) {
	return nil, fmt.Errorf("get operation is not supported in BigQuery")
}

// List fetches multiple entities by IDs (not supported in BigQuery)
func (db *BqDatabase) List(factory entity.EntityFactory, entityIDs []string, keys ...string) ([]entity.Entity, error) {
	return nil, fmt.Errorf("list operation is not supported in BigQuery")
}

// Exists checks if an entity exists by ID (not supported in BigQuery)
func (db *BqDatabase) Exists(factory entity.EntityFactory, entityID string, keys ...string) (bool, error) {
	return false, fmt.Errorf("exists operation is not supported in BigQuery")
}

// Insert inserts a new entity (not supported in BigQuery)
func (db *BqDatabase) Insert(entity entity.Entity) (entity.Entity, error) {
	return nil, fmt.Errorf("insert operation is not supported in BigQuery")
}

// Update updates an existing entity (not supported in BigQuery)
func (db *BqDatabase) Update(entity entity.Entity) (entity.Entity, error) {
	return nil, fmt.Errorf("update operation is not supported in BigQuery")
}

// Upsert inserts or updates an entity (not supported in BigQuery)
func (db *BqDatabase) Upsert(entity entity.Entity) (entity.Entity, error) {
	return nil, fmt.Errorf("upsert operation is not supported in BigQuery")
}

// Delete deletes an entity by ID (not supported in BigQuery)
func (db *BqDatabase) Delete(factory entity.EntityFactory, entityID string, keys ...string) error {
	return fmt.Errorf("delete operation is not supported in BigQuery")
}

// Bulk Operations (No-op implementations) -------------------------------------

func (db *BqDatabase) BulkInsert(entities []entity.Entity) (int64, error) {

	if len(entities) == 0 {
		return 0, nil
	}

	batchSize := config.Get().BigQueryBatchSize()
	timeout := time.Duration(config.Get().CfgBigQueryBatchTimeouSec()) * time.Second // Set timeout duration for BQ bulk insert operation
	totalInserted := 0

	for start := 0; start < len(entities); start += batchSize {
		end := start + batchSize
		if end > len(entities) {
			end = len(entities)
		}
		batch := entities[start:end]

		// Create a context with timeout for each batch
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if _, err := db.bulkInsertInternal(ctx, batch); err != nil {
			return int64(totalInserted), err
		}
		totalInserted += len(batch)
	}

	return int64(totalInserted), nil
}

func (db *BqDatabase) bulkInsertInternal(ctx context.Context, entities []entity.Entity) (int, error) {
	if len(entities) == 0 {
		return 0, nil
	}

	inserter := db.client.Dataset(db.dataSet).Table(entities[0].TABLE()).Inserter()
	// Insert the records
	if err := inserter.Put(ctx, entities); err != nil {
		return 0, err
	}

	return len(entities), nil
}

func (db *BqDatabase) BulkUpdate(entities []entity.Entity) (int64, error) {
	return 0, fmt.Errorf("BulkUpdate operation is not supported in BigQuery")
}

func (db *BqDatabase) BulkUpsert(entities []entity.Entity) (int64, error) {
	return 0, fmt.Errorf("BulkUpsert operation is not supported in BigQuery")
}

func (db *BqDatabase) BulkDelete(factory entity.EntityFactory, entityIDs []string, keys ...string) (int64, error) {
	return 0, fmt.Errorf("BulkDelete operation is not supported in BigQuery")
}

func (db *BqDatabase) SetField(factory entity.EntityFactory, entityID string, field string, value any, keys ...string) error {
	return fmt.Errorf("SetField operation is not supported in BigQuery")
}

func (db *BqDatabase) SetFields(factory entity.EntityFactory, entityID string, fields map[string]any, keys ...string) error {
	return fmt.Errorf("SetFields operation is not supported in BigQuery")
}

func (db *BqDatabase) BulkSetFields(factory entity.EntityFactory, field string, values map[string]any, keys ...string) (int64, error) {
	return 0, fmt.Errorf("BulkSetFields operation is not supported in BigQuery")
}

// Query Operations ------------------------------------------------------------

// Query returns an instance of BqDatabaseQuery to build and execute a query
func (db *BqDatabase) Query(factory entity.EntityFactory) database.IQuery {
	return &bqDatabaseQuery{
		client:         db.client,
		factory:        factory,
		tablePrefix:    fmt.Sprintf("%s.%s", db.projectId, db.dataSet),
		fieldTagToType: buildFieldsTypesMap(factory()),
	}
}

func (db *BqDatabase) QueryAdvanced(factory entity.EntityFactory) database.IAdvancedQuery {

	bq := &bqDatabaseQuery{
		client:         db.client,
		factory:        factory,
		tablePrefix:    fmt.Sprintf("%s.%s", db.projectId, db.dataSet),
		fieldTagToType: buildFieldsTypesMap(factory()),
		bqFieldInfo:    buildAnalyticFieldsdMap(factory()),
	}
	return bq
}

// DDL Operations (No-op) ------------------------------------------------------

// ExecuteDDL is a no-op for BigQuery
func (db *BqDatabase) ExecuteDDL(ddl map[string][]string) error {
	return fmt.Errorf("ExecuteDDL is not supported in BigQuery")
}

// ExecuteSQL is a no-op for BigQuery
func (db *BqDatabase) ExecuteSQL(sql string, args ...any) (int64, error) {
	return 0, fmt.Errorf("ExecuteSQL is not supported in BigQuery")
}

// ExecuteQuery executes a native SQL query and returns the result as JSON
func (db *BqDatabase) ExecuteQuery(source, sql string, args ...any) (results []entity.Json, err error) {
	ctx := context.Background()
	query := db.client.Query(sql)

	// Add arguments (parameters) to the query, if any
	for i, arg := range args {
		query.Parameters = append(query.Parameters, bigquery.QueryParameter{
			Name:  fmt.Sprintf("param%d", i+1),
			Value: arg,
		})
	}

	// Execute the query
	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading query result: %v", err)
		}

		//TODO check how to implement

		// Convert BigQuery row to JSON format
		//results = append(results, entity.Json(row))
	}

	return
}

// DropTable is a no-op for BigQuery
func (db *BqDatabase) DropTable(table string) error {
	return fmt.Errorf("DropTable is not supported in BigQuery")
}

// PurgeTable is a no-op for BigQuery
func (db *BqDatabase) PurgeTable(table string) error {
	return fmt.Errorf("PurgeTable is not supported in BigQuery")
}
