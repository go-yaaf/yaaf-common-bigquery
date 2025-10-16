package bqdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-yaaf/yaaf-common/config"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type tableStream struct {
	stream    *managedwriter.ManagedStream
	bqSchema  bigquery.Schema
	msgDesc   protoreflect.MessageDescriptor
	tableFQN  string // projects/{p}/datasets/{d}/tables/{t}
	createdAt time.Time
}

// BqDatabase struct implementing IDatabase for BigQuery
type BqDatabase struct {
	client *bigquery.Client
	projectId,
	dataSet string

	// SWA client + per-table streams (lazy-init and cached).
	streamMu    sync.RWMutex
	mw          *managedwriter.Client
	streamCache map[string]*tableStream // key: tableName
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
func NewBqDatabase(uri string) (*BqDatabase, error) {
	const schemaPrefix = "bq://"
	if !strings.HasPrefix(uri, schemaPrefix) {
		return nil, fmt.Errorf("invalid URI: must start with %q", schemaPrefix)
	}
	trimmed := strings.TrimPrefix(uri, schemaPrefix)
	parts := strings.Split(trimmed, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("invalid URI format; expected bq://<project>:<dataset>")
	}
	projectID, dataset := parts[0], parts[1]

	ctx := context.Background()

	// BigQuery data client
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}

	// Storage Write API client (eagerly initialized here)
	mwClient, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		_ = bqClient.Close()
		return nil, fmt.Errorf("managedwriter.NewClient: %w", err)
	}

	return &BqDatabase{
		client:      bqClient,
		projectId:   projectID,
		dataSet:     dataset,
		mw:          mwClient,
		streamCache: make(map[string]*tableStream),
	}, nil
}

// Close Closer includes method Close()
func (db *BqDatabase) Close() error {
	var firstErr error
	if db.mw != nil {
		if err := db.mw.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.client != nil {
		if err := db.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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

// Bulk Operations -------------------------------------

func (db *BqDatabase) BulkInsert(entities []entity.Entity) (int64, error) {

	if len(entities) == 0 {
		return 0, nil
	}

	batchSize := config.Get().BigQueryBatchSize()
	timeout := time.Duration(config.Get().CfgBigQueryBatchTimeoutSec()) * time.Second // Set timeout duration for BQ bulk insert operation
	totalInserted := 0

	for start := 0; start < len(entities); start += batchSize {

		end := min(start+batchSize, len(entities))
		batch := entities[start:end]

		// Create a context with timeout for each batch
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		_, err := db.bulkInsertInternal(ctx, batch)
		cancel()

		if err != nil {
			return int64(totalInserted), err
		}

		totalInserted += len(batch)

	}

	return int64(totalInserted), nil
}

func (db *BqDatabase) bulkInsertInternal(ctx context.Context, batch []entity.Entity) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}
	table := batch[0].TABLE()

	ts, err := db.getOrInitStream(ctx, table)
	if err != nil {
		return 0, err
	}

	// Build dynamic protos once
	msgs := make([]*dynamicpb.Message, 0, len(batch))
	for _, e := range batch {
		m, err := entityToProto(ts.msgDesc, e)
		if err != nil {
			return 0, fmt.Errorf("entityToProto: %w", err)
		}
		msgs = append(msgs, m)
	}

	// Encode once (so retries don’t re-marshal)
	encoded := make([][]byte, len(msgs))
	for j, m := range msgs {
		b, encErr := proto.Marshal(m)
		if encErr != nil {
			return 0, fmt.Errorf("proto.Marshal: %w", encErr)
		}
		encoded[j] = b
	}

	backoff := []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 800 * time.Millisecond, 1500 * time.Millisecond}
	var lastErr error

	for attempt := 0; attempt < len(backoff)+1; attempt++ {
		_, err = ts.stream.AppendRows(ctx, encoded)
		if err == nil {
			// SUCCESS: return number of rows written
			return len(encoded), nil
		}
		lastErr = err

		// Respect context; don’t keep retrying on deadline/cancel
		if ctx.Err() != nil {
			break
		}

		// Sleep only if we will retry again
		if attempt < len(backoff) {
			time.Sleep(backoff[attempt])
		}
	}

	return 0, fmt.Errorf("AppendRows failed after retries: %w", lastErr)
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

// DropTable Drop table and indexes
func (db *BqDatabase) DropTable(table string) (err error) {
	return nil
}

// Query Operations ------------------------------------------------------------

// Query returns an instance of BqDatabaseQuery to build and execute a query
func (db *BqDatabase) Query(factory entity.EntityFactory) database.IQuery {
	return &bqDatabaseQuery{
		client:        db.client,
		factory:       factory,
		tablePrefix:   fmt.Sprintf("%s.%s", db.projectId, db.dataSet),
		jsonTagToType: buildFieldsTypesMap(factory()),
		bqFieldInfo:   buildAnalyticFieldsdMap(factory()),
	}
}

func (db *BqDatabase) AdvancedQuery(factory entity.EntityFactory) database.IAdvancedQuery {

	bq := &bqDatabaseQuery{
		client:        db.client,
		factory:       factory,
		tablePrefix:   fmt.Sprintf("%s.%s", db.projectId, db.dataSet),
		jsonTagToType: buildFieldsTypesMap(factory()),
		bqFieldInfo:   buildAnalyticFieldsdMap(factory()),
	}
	return bq
}

// DDL Operations (No-op) ------------------------------------------------------
// PurgeTable is a no-op for BigQuery
func (db *BqDatabase) PurgeTable(table string) error {
	return fmt.Errorf("PurgeTable is not supported in BigQuery")
}

// ExecuteDDL tailored for CREATE_IF_NOT_EXIST sharded
// flow-record table.
func (db *BqDatabase) ExecuteDDL(ddl map[string][]string) error {

	shardKey := ddl["shardKey"][0]
	sql := ddl["sql"][0]
	tableName := ddl["tableName"][0]

	tableId := fmt.Sprintf("%s-%s", tableName, shardKey)
	tableFullQualifyingName := fmt.Sprintf("%s.%s.%s", db.projectId, db.dataSet, tableId)
	// Reference the dataset and table
	dataset := db.client.Dataset(db.dataSet)
	table := dataset.Table(tableId)

	// Check if the table exists
	_, err := table.Metadata(context.Background())
	if err == nil {
		// Table already exists, no need to create
		return nil
	} else {
		if !strings.Contains(strings.ToLower(err.Error()), "not found") {
			return fmt.Errorf("failed to check if table %s exists: %v", tableId, err)
		}
	}

	sql = fmt.Sprintf(sql, fmt.Sprintf("`%s`", tableFullQualifyingName))

	// Run the query
	query := db.client.Query(sql)
	job, err := query.Run(context.Background())
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	// Wait for the query to complete
	status, err := job.Wait(context.Background())
	if err != nil {
		return fmt.Errorf("create flow data table %s failed with error %v", tableId, err)
	}

	if err := status.Err(); err != nil {
		return fmt.Errorf("create flow data table %s failed with error %v", tableId, err)
	}

	return nil

}

// ExecuteSQL runs a parameterized SQL query on BigQuery and returns the number of affected rows.
//
// This function replaces the `{table_name}` placeholder in the query with a fully qualified BigQuery table name,
// executes the query, waits for it to complete, and retrieves the number of affected rows.
//
// Parameters:
// - sql: string - The SQL query to execute, containing `{table_name}` as a placeholder.
// - args: ...any - A variadic parameter where the first argument must be the table name.
//
// Returns:
// - int64 - The number of rows affected by the query (for DELETE, UPDATE, or INSERT statements).
// - error - An error if query execution fails.
//
// Function Workflow:
// 1. **Validates input**: Ensures that a table name is provided.
// 2. **Constructs the fully qualified table name**: `project.dataset.table`.
// 3. **Replaces `{table_name}` in the SQL query** with the resolved table name.
// 4. **Uses `context.WithTimeout` (2 minutes)** to prevent infinite waiting.
// 5. **Executes the query using `db.client.Query(sql).Run(ctx)`**.
// 6. **Waits for the query to complete within the timeout** using `job.Wait(ctx)`.
// 7. **Checks for errors** after execution to ensure successful completion.
// 8. **Retrieves job statistics** and safely extracts `NumDMLAffectedRows`.
// 9. **Returns the number of affected rows or an error if execution fails**.
//
// Important Notes:
// - This function is designed for **DML operations** (`DELETE`, `UPDATE`, `INSERT`).
// - It ensures **graceful handling of missing table names** instead of panicking.
// - **Context timeout** prevents **indefinite blocking** in case BigQuery hangs.
// - **Safe type assertion** is used to avoid runtime panics when accessing query statistics.
func (db *BqDatabase) ExecuteSQL(sql string, args ...any) (int64, error) {
	// Validate input: Ensure at least one argument (table name) is provided
	if len(args) == 0 {
		panic("missing required table name argument in ExecuteSQL")
	}

	// Construct the fully qualified table name: project.dataset.table
	tableFullQualifiedName := fmt.Sprintf("`%s.%s.%s`", db.projectId, db.dataSet, args[0])

	// Replace {table_name} placeholder in SQL with the actual table name
	sql = strings.Replace(sql, "{table_name}", tableFullQualifiedName, 1)

	// Set a timeout (2 minutes) to prevent infinite waiting
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Ensure cleanup

	// Prepare and execute the query
	query := db.client.Query(sql)
	job, err := query.Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to run query: %w", err)
	}

	// Wait for the query to complete within the timeout
	status, err := job.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("query execution failed: %w", err)
	}

	// Check for execution errors
	if err := status.Err(); err != nil {
		return 0, fmt.Errorf("query execution completed with error: %w", err)
	}

	// Retrieve job statistics
	jobStatus, err := job.Status(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve job status: %w", err)
	}

	// Extract the number of affected rows safely
	queryStats, ok := jobStatus.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		return 0, fmt.Errorf("unexpected job statistics type, failed to extract affected rows")
	}

	return queryStats.NumDMLAffectedRows, nil
}

// ExecuteQuery executes a native SQL query and returns the result as JSON
func (db *BqDatabase) ExecuteQuery(source, sql string, args ...any) (results []entity.Json, err error) {

	// Validate input: Ensure at least one argument (table name) is provided
	if len(args) == 0 {
		panic("missing required table name argument in ExecuteSQL")
	}

	// Construct the fully qualified table name: project.dataset.table
	tableFullQualifiedName := fmt.Sprintf("`%s.%s.%s`", db.projectId, db.dataSet, args[0])

	// Replace {table_name} placeholder in SQL with the actual table name
	sql = strings.Replace(sql, "{table_name}", tableFullQualifiedName, 1)

	ctx := context.Background()
	query := db.client.Query(sql)

	if len(args) > 1 {
		for i := 1; i < len(args); i++ {
			pn := fmt.Sprintf("param%d", i)
			query.Parameters = []bigquery.QueryParameter{
				{Name: pn, Value: args[i]},
			}
		}
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

		converted := make(entity.Json)
		for k, v := range row {
			converted[k] = v
		}
		results = append(results, converted)
	}

	return
}

func (db *BqDatabase) getOrInitStream(ctx context.Context, tableName string) (*tableStream, error) {
	// Fast path: return cached stream if present
	db.streamMu.RLock()
	if ts, ok := db.streamCache[tableName]; ok {
		db.streamMu.RUnlock()
		return ts, nil
	}
	db.streamMu.RUnlock()

	// 1) Read live table metadata (for schema)
	tbl := db.client.Dataset(db.dataSet).Table(tableName)
	md, err := tbl.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("table metadata for %s: %w", tableName, err)
	}

	// 2) Build proto descriptor from BQ schema
	storageSchema, err := adapt.BQSchemaToStorageTableSchema(md.Schema)
	if err != nil {
		return nil, fmt.Errorf("BQSchemaToStorageTableSchema: %w", err)
	}

	// IMPORTANT: sanitize scope for proto identifier rules (no dashes, etc.)
	scope := protoIdentSafe(fmt.Sprintf("%s_%s_%s",
		db.projectId, db.dataSet, tableName,
	))

	desc, err := adapt.StorageSchemaToProto3Descriptor(storageSchema, scope)
	if err != nil {
		return nil, fmt.Errorf("StorageSchemaToProto3Descriptor: %w", err)
	}
	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("schema descriptor for %s is not a message", tableName)
	}

	// 3) Destination resource name for the table (THIS is the missing dest)
	dest := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", db.projectId, db.dataSet, tableName)

	// 4) v1.63.1 requires *descriptorpb.DescriptorProto; NormalizeDescriptor returns that.
	descriptorProto, err := adapt.NormalizeDescriptor(msgDesc)
	if err != nil {
		return nil, fmt.Errorf("NormalizeDescriptor: %w", err)
	}

	// 5) Open managed stream bound to the table + schema
	stream, err := db.mw.NewManagedStream(
		ctx,
		managedwriter.WithDestinationTable(dest),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return nil, fmt.Errorf("NewManagedStream(%s): %w", dest, err)
	}

	ts := &tableStream{
		stream:    stream,
		bqSchema:  md.Schema,
		msgDesc:   msgDesc,
		tableFQN:  dest,
		createdAt: time.Now(),
	}

	// 6) Cache with race protection
	db.streamMu.Lock()
	if cur := db.streamCache[tableName]; cur == nil {
		db.streamCache[tableName] = ts
	} else {
		_ = stream.Close()
		ts = cur
	}
	db.streamMu.Unlock()

	return ts, nil
}
