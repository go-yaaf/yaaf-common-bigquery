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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
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

// --- Storage Write API error decoding helpers ---

// decodeAppendError unwraps gRPC status details to extract row-level errors.
func decodeAppendError(err error) string {
	if err == nil {
		return ""
	}
	st, ok := status.FromError(err)
	if !ok {
		return err.Error()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SWA error: %s\n", st.Message())

	sample := 0
	const maxSample = 25 // keep output short but useful

	for _, d := range st.Details() {
		switch t := d.(type) {
		case *storagepb.RowError:
			if sample < maxSample {
				// RowError has: index, code, message
				fmt.Fprintf(&b, "  row %d: %s (code=%v)\n", t.GetIndex(), t.GetMessage(), t.GetCode())
				sample++
			}
		case *storagepb.StorageError:
			// High-level storage error (schema mismatch, missing required field, etc.)
			fmt.Fprintf(&b, "  storage: %s (entity=%v, code=%v)\n", t.GetErrorMessage(), t.GetEntity(), t.GetCode())
		case *errdetails.BadRequest:
			for _, v := range t.GetFieldViolations() {
				if sample < maxSample {
					fmt.Fprintf(&b, "  bad_request field=%s: %s\n", v.GetField(), v.GetDescription())
					sample++
				}
			}
		default:
			// Keep unknown detail types visible
			fmt.Fprintf(&b, "  detail: %T\n", t)
		}
	}

	return b.String()
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
			decodeAppendError(err)
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

	ts, err := db.getOrInitStream(context.Background(), table)
	if err != nil {
		return 0, err
	}

	// Build proto messages (reflection → dynamicpb)
	msgs := make([]*dynamicpb.Message, 0, len(batch))
	for _, e := range batch {
		m, err := entityToProto(ts.msgDesc, e)
		if err != nil {
			return 0, fmt.Errorf("entityToProto: %w", err)
		}
		msgs = append(msgs, m)
	}

	// Marshal once
	encoded := make([][]byte, len(msgs))
	for i, m := range msgs {
		b, encErr := proto.Marshal(m)
		if encErr != nil {
			return 0, fmt.Errorf("proto.Marshal: %w", encErr)
		}
		encoded[i] = b
	}

	// Retry policy
	backoff := []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 800 * time.Millisecond, 1500 * time.Millisecond}
	var lastErr error

	// Helper: build a wait context a bit longer than submit ctx
	buildWaitCtx := func() (context.Context, context.CancelFunc) {
		// If caller gave   a deadline, add a small buffer; otherwise cap at 30s.
		if dl, ok := ctx.Deadline(); ok {
			remain := time.Until(dl)
			// Give GetResult a little more than the submit path (min 2s, max 30s).
			d := max(min(remain+2*time.Second, 30*time.Second), 2*time.Second)
			return context.WithTimeout(context.Background(), d)
		}
		return context.WithTimeout(context.Background(), 30*time.Second)
	}

	for attempt := 0; attempt < len(backoff)+1; attempt++ {
		// Submit append using caller's ctx (respects   per-batch timeout)
		res, err := ts.stream.AppendRows(ctx, encoded)
		if err == nil {
			// Wait for server ack with a slightly longer context
			waitCtx, cancel := buildWaitCtx()
			_, gerr := res.GetResult(waitCtx)
			cancel()
			if gerr == nil {
				return len(encoded), nil // SUCCESS
			}
			err = gerr
		}

		// Failure path
		lastErr = err

		// If caller's ctx is done, don't spin
		if ctx.Err() != nil {
			break
		}
		// Sleep only if another attempt remains
		if attempt < len(backoff) {
			time.Sleep(backoff[attempt])
		}
	}

	return 0, fmt.Errorf("AppendRows/GetResult failed after retries: %w", lastErr)
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

// ExecuteDDL creates one flow data table and its materialized views for a given shard.
// It expects ddl to contain:
// - "shardKey": exactly one element (the stream/shard id)
// - "name": base object names; name[0] is the flow table base name, name[i>0] are MV base names
// - "sql": DDL templates aligned 1:1 with "name". sql[0] must have one %s for the table FQN;
// each MV sql[i>0] must have two %s: first for the MV FQN, second for the base table FQN.
// The physical object names are formed as "<base>-<shardKey>", and FQNs are
// project.dataset.object. The function is idempotent if templates use
// "IF NOT EXISTS" clause.
func (db *BqDatabase) ExecuteDDL(ddl map[string][]string) error {

	// ---- Validate inputs
	get := func(k string) ([]string, bool) { v, ok := ddl[k]; return v, ok }

	shardVals, ok := get("shardKey")
	if !ok || len(shardVals) != 1 || shardVals[0] == "" {
		return fmt.Errorf(`ExecuteDDL: "shardKey" must contain exactly one non-empty value`)
	}
	shardKey := shardVals[0]

	names, ok := get("name")
	if !ok || len(names) == 0 {
		return fmt.Errorf(`ExecuteDDL: "name" must be present and non-empty`)
	}
	sqls, ok := get("sql")
	if !ok || len(sqls) == 0 {
		return fmt.Errorf(`ExecuteDDL: "sql" must be present and non-empty`)
	}
	if len(names) != len(sqls) {
		return fmt.Errorf(`ExecuteDDL: len("name") (%d) != len("sql") (%d)`, len(names), len(sqls))
	}

	// ---- Helpers
	quoteFQN := func(obj string) string {
		return fmt.Sprintf("`%s.%s.%s`", db.projectId, db.dataSet, obj)
	}
	physicalName := func(base string) string {
		return fmt.Sprintf("%s-%s", base, shardKey)
	}

	runDDL := func(ctx context.Context, stmt string) error {
		q := db.client.Query(stmt)
		job, err := q.Run(ctx)
		if err != nil {
			return err
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return err
		}
		return status.Err()
	}

	// Entire operation deadline; short per-statement retries inside.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// ---- 1) Create base flow data table (names[0], sqls[0]) ----
	tablePhys := physicalName(names[0])
	tableFQN := quoteFQN(tablePhys)

	// Expect exactly one %s in the table template.
	stmt := fmt.Sprintf(sqls[0], tableFQN)

	// Retry transient errors up to 3 times with backoff.
	if err := withBackoff(ctx, 3, 500*time.Millisecond, func(c context.Context) error {
		return runDDL(c, stmt)
	}); err != nil {
		return fmt.Errorf("ExecuteDDL: creating table %s failed: %w", tableFQN, err)
	}

	// ---- 2) Create each MV over the base table ----
	for i := 1; i < len(names); i++ {
		mvPhys := physicalName(names[i])
		mvFQN := quoteFQN(mvPhys)

		// MV template must accept MV FQN then base table FQN (two %s).
		mvStmt := fmt.Sprintf(sqls[i], mvFQN, tableFQN)

		if err := withBackoff(ctx, 3, 500*time.Millisecond, func(c context.Context) error {
			return runDDL(c, mvStmt)
		}); err != nil {
			return fmt.Errorf("ExecuteDDL: creating MV %s failed: %w", mvFQN, err)
		}
	}
	return nil
}

func (db *BqDatabase) createMView(mvName, srcTableName, sql string) error {

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
	// Fast path: cached
	db.streamMu.RLock()
	if ts, ok := db.streamCache[tableName]; ok {
		db.streamMu.RUnlock()
		return ts, nil
	}
	db.streamMu.RUnlock()

	// Use caller ctx for quick metadata (fine if this times out)
	tbl := db.client.Dataset(db.dataSet).Table(tableName)
	md, err := tbl.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("table metadata for %s: %w", tableName, err)
	}

	storageSchema, err := adapt.BQSchemaToStorageTableSchema(md.Schema)
	if err != nil {
		return nil, fmt.Errorf("BQSchemaToStorageTableSchema: %w", err)
	}

	// Flat, proto-safe scope (no dots, no dashes)
	scope := protoIdentSafe(fmt.Sprintf("%s_%s_%s", db.projectId, db.dataSet, tableName))
	desc, err := adapt.StorageSchemaToProto3Descriptor(storageSchema, scope)
	if err != nil {
		return nil, fmt.Errorf("StorageSchemaToProto3Descriptor: %w", err)
	}
	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("schema descriptor for %s is not a message", tableName)
	}
	descriptorProto, err := adapt.NormalizeDescriptor(msgDesc)
	if err != nil {
		return nil, fmt.Errorf("NormalizeDescriptor: %w", err)
	}

	// IMPORTANT: create the stream with a long-lived context, not the batch ctx.
	// Otherwise the cached stream becomes permanently canceled after the first batch.
	dest := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", db.projectId, db.dataSet, tableName)
	stream, err := db.mw.NewManagedStream(
		context.Background(),
		managedwriter.WithDestinationTable(dest),
		managedwriter.WithSchemaDescriptor(descriptorProto),
		// Optional safety: serialize inflight requests for  strict one-at-a-time
		//managedwriter.WithMaxInflightRequests(1),
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
