package bigquerydb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	"google.golang.org/api/iterator"
)

type FlowRecord struct {
	BaseEntity
	FlowId        int64  `json:"flow_id"`
	StreamId      string `json:"stream_id"`
	DeviceId      string `json:"device_id"`
	StartTime     int64  `json:"start_time"`
	EndTime       int64  `json:"end_time"`
	SrcIP         string `json:"src_ip"`
	SrcPort       int    `json:"src_port"`
	DstIP         string `json:"dst_ip"`
	DstPort       int    `json:"dst_port"`
	PcktToSrv     int    `json:"pckt_to_srv"`
	PcktToClient  int    `json:"pckt_to_client"`
	BytesToSrv    int    `json:"bytes_to_srv"`
	BytesToClient int    `json:"bytes_to_client"`
	Proto         string `json:"proto"`
	Pcap          string `json:"pcap"`
}

// BqDatabaseQuery struct for building and executing queries on BigQuery
type BqDatabaseQuery struct {
	client       *bigquery.Client
	dataSet      string
	factory      EntityFactory
	allFilters   []database.QueryFilter
	limit        int
	offset       int
	orderBy      []string
	selectFields []string
	tablePrefix  string
}

// Apply adds a callback to apply on each result entity in the query
func (q *BqDatabaseQuery) Apply(cb func(in Entity) Entity) database.IQuery {
	// No-op for BigQuery as we don’t need callback transformation at this level
	return q
}

// Filter adds a single field filter
func (q *BqDatabaseQuery) Filter(filter database.QueryFilter) database.IQuery {
	if filter.IsActive() {
		q.allFilters = append(q.allFilters, filter)
	}
	return q
}

// Range adds a time frame filter on a specific time field
func (q *BqDatabaseQuery) Range(field string, from Timestamp, to Timestamp) database.IQuery {
	// BigQuery expects time filters to be part of the WHERE clause
	timeFilter := database.F(field).Between(from, to)
	return q.Filter(timeFilter)
}

// MatchAll adds filters that must all be satisfied (AND)
func (q *BqDatabaseQuery) MatchAll(filters ...database.QueryFilter) database.IQuery {
	for _, filter := range filters {
		if filter.IsActive() {
			q.allFilters = append(q.allFilters, filter)
		}
	}
	return q
}

// MatchAny adds filters where any can be satisfied (OR)
func (q *BqDatabaseQuery) MatchAny(filters ...database.QueryFilter) database.IQuery {
	// In BigQuery, this can be converted to a series of OR conditions
	orFilter := fmt.Sprintf("(%s)", q.buildFilterConditions(filters, " OR "))
	return q.Filter(database.F("or_condition").Eq(orFilter))
}

// Sort adds sort order by field
func (q *BqDatabaseQuery) Sort(sort string) database.IQuery {
	if sort == "" {
		return q
	}

	// Add sorting
	if strings.HasSuffix(sort, "-") {
		q.orderBy = append(q.orderBy, fmt.Sprintf("%s DESC", sort[:len(sort)-1]))
	} else {
		q.orderBy = append(q.orderBy, fmt.Sprintf("%s ASC", sort))
	}
	return q
}

// Page sets the page number for pagination
func (q *BqDatabaseQuery) Page(page int) database.IQuery {
	q.offset = (page - 1) * q.limit
	return q
}

// Limit sets the page size limit for pagination
func (q *BqDatabaseQuery) Limit(limit int) database.IQuery {
	q.limit = limit
	return q
}

// List executes a query to get a list of entities by their IDs
func (q *BqDatabaseQuery) List(entityIDs []string, keys ...string) ([]Entity, error) {
	// Stub for compatibility with the interface, BigQuery doesn’t support direct ID-based fetching like traditional DBs
	return nil, fmt.Errorf("list by IDs is not supported in BigQuery")
}

// Find executes the query based on the criteria, order, and pagination
func (q *BqDatabaseQuery) Find(keys ...string) ([]Entity, int64, error) {
	ctx := context.Background()

	// Build the SQL query from filters, sorting, etc.
	queryString := q.buildSQL()

	// Execute the query
	query := q.client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("query execution failed: %v", err)
	}

	// Process rows and convert to Entity objects
	var entities []Entity

	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, 0, err
		}

		entity := q.factory()
		mapToStruct(row, entity) // Assume this function maps row data to Entity
		entities = append(entities, entity)
	}

	return entities, int64(len(entities)), nil
}

// Select executes the query and retrieves specific fields
func (q *BqDatabaseQuery) Select(fields ...string) ([]Json, error) {
	ctx := context.Background()

	// Build the SQL query from filters, sorting, and selected fields
	q.selectFields = fields
	queryString := q.buildSQL()

	// Execute the query
	query := q.client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %v", err)
	}

	// Process rows and return them as JSON
	var results []Json
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		//TODO check how to implement

		//results = append(results, Json(row))
	}

	return results, nil
}

// Count executes the query based on the criteria and returns only the count of matching rows
func (q *BqDatabaseQuery) Count(keys ...string) (int64, error) {
	ctx := context.Background()

	// Build the SQL query to count rows
	queryString := q.buildSQLForCount()

	// Execute the query
	query := q.client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("count query execution failed: %v", err)
	}

	var count int64
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, err
		}
		if len(row) > 0 {
			count = row[0].(int64)
		}
	}

	return count, nil
}

// Aggregation executes an aggregation function on the field (count, avg, sum, etc.)
func (q *BqDatabaseQuery) Aggregation(field string, function database.AggFunc, keys ...string) (float64, error) {
	ctx := context.Background()

	// Build SQL for aggregation
	queryString := fmt.Sprintf("SELECT %s(%s) FROM `your_project.dataset.table`", function, field)

	// Execute the query
	query := q.client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("aggregation query execution failed: %v", err)
	}

	var result float64
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, err
		}
		if len(row) > 0 {
			result = row[0].(float64)
		}
	}

	return result, nil
}

// GroupAggregation Execute the query based on the criteria, order, and pagination and return the aggregated value per group.
// The data point is a calculation of the provided function on the selected field, each data point includes the number of documents and the calculated value.
// The total is the sum of all calculated values in all the buckets.
func (q *BqDatabaseQuery) GroupAggregation(field string, function database.AggFunc, keys ...string) (map[any]Tuple[int64, float64], float64, error) {
	// Check if the function is supported
	validFunctions := map[database.AggFunc]string{
		database.COUNT: "COUNT",
		database.SUM:   "SUM",
		database.AVG:   "AVG",
		database.MIN:   "MIN",
		database.MAX:   "MAX",
	}
	aggFunc, ok := validFunctions[function]
	if !ok {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}

	result := make(map[any]Tuple[int64, float64])
	total := float64(0)

	// Build the SQL query for BigQuery
	groupField := field // Field by which to group
	aggField := field   // Field to perform the aggregation on

	// If the function is COUNT, we'll count the number of rows, otherwise, we'll perform the function on the field
	selectField := "*"
	if function != database.COUNT {
		selectField = fmt.Sprintf("CAST(%s AS FLOAT64)", aggField)
	}

	tableName := q.factory().TABLE()

	sql := fmt.Sprintf(`SELECT %s(%s) as value, %s as groupField FROM %s.%s GROUP BY groupField`, aggFunc, selectField, groupField, q.dataSet, tableName)

	// Execute the query
	ctx := context.Background()
	query := q.client.Query(sql)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %v", err)
	}

	// Process the result
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("error reading query result: %v", err)
		}

		// Extract the group value and the aggregation value
		groupValue := row["groupField"]
		aggValue, ok := row["value"].(float64)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected value type for aggregation result")
		}

		// Add the result to the map
		result[groupValue] = Tuple[int64, float64]{Key: 1, Value: aggValue}
		total += aggValue
	}

	return result, total, nil
}

// GroupCount Execute the query based on the criteria, grouped by field and return count per group
func (q *BqDatabaseQuery) GroupCount(field string, keys ...string) (map[any]int64, int64, error) {
	result := make(map[any]int64)
	total := int64(0)

	// Build the SQL query for grouping by the field and counting occurrences
	tableName := q.factory().TABLE()
	sql := fmt.Sprintf(`SELECT COUNT(*) as cnt, %s as groupField FROM %s.%s GROUP BY groupField`, field, q.dataSet, tableName)

	// Execute the query
	ctx := context.Background()
	query := q.client.Query(sql)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %v", err)
	}

	// Process the result
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("error reading query result: %v", err)
		}

		// Extract the group value and the count
		groupValue := row["groupField"]
		countValue, ok := row["cnt"].(int64)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected value type for count result")
		}

		result[groupValue] = countValue
		total += countValue
	}

	return result, total, nil
}

// Histogram returns a time series data points based on the time field, supported intervals: Minute, Hour, Day, Week, Month
// The data point is a calculation of the provided function on the selected field, each data point includes the number of documents and the calculated value
func (q *BqDatabaseQuery) Histogram(field string, function database.AggFunc, timeField string, interval time.Duration, keys ...string) (map[Timestamp]Tuple[int64, float64], float64, error) {
	validFunctions := map[database.AggFunc]string{
		database.COUNT: "COUNT",
		database.SUM:   "SUM",
		database.AVG:   "AVG",
		database.MIN:   "MIN",
		database.MAX:   "MAX",
	}
	aggFunc, ok := validFunctions[function]
	if !ok {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}

	result := make(map[Timestamp]Tuple[int64, float64])
	total := float64(0)

	// Determine the interval format for BigQuery's date_trunc function
	intervalFormat := q.calculateDatePart(interval)

	// Build the SQL query for histogram aggregation
	tableName := q.factory().TABLE()
	sql := fmt.Sprintf(`SELECT %s(%s) as value, DATE_TRUNC(%s, INTERVAL %s) as timeGroup FROM %s.%s GROUP BY timeGroup ORDER BY timeGroup`,
		aggFunc, field, timeField, intervalFormat, q.dataSet, tableName)

	// Execute the query
	ctx := context.Background()
	query := q.client.Query(sql)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %v", err)
	}

	// Process the result
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("error reading query result: %v", err)
		}

		// Extract the time group and the aggregated value
		timeGroup, ok := row["timeGroup"].(Timestamp)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected value type for time group")
		}

		aggValue, ok := row["value"].(float64)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected value type for aggregation result")
		}

		result[timeGroup] = Tuple[int64, float64]{Key: 1, Value: aggValue}
		total += aggValue
	}

	return result, total, nil
}

// Histogram2D returns a two-dimensional time series data points based on the time field and an additional dimension
// The supported aggregation functions are: count, avg, sum, min, max
func (q *BqDatabaseQuery) Histogram2D(field string, function database.AggFunc, dim string, timeField string, interval time.Duration, keys ...string) (map[Timestamp]map[any]Tuple[int64, float64], float64, error) {
	validFunctions := map[database.AggFunc]string{
		database.COUNT: "COUNT",
		database.SUM:   "SUM",
		database.AVG:   "AVG",
		database.MIN:   "MIN",
		database.MAX:   "MAX",
	}
	aggFunc, ok := validFunctions[function]
	if !ok {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}

	result := make(map[Timestamp]map[any]Tuple[int64, float64])
	total := float64(0)

	// Determine the interval format for BigQuery's date_trunc function
	intervalFormat := q.calculateDatePart(interval)

	// Build the SQL query for 2D histogram aggregation
	tableName := q.factory().TABLE()
	sql := fmt.Sprintf(`SELECT %s(%s) as value, DATE_TRUNC(%s, INTERVAL %s) as timeGroup, %s as dimension FROM %s.%s GROUP BY timeGroup, dimension ORDER BY timeGroup`,
		aggFunc, field, timeField, intervalFormat, dim, q.dataSet, tableName)

	// Execute the query
	ctx := context.Background()
	query := q.client.Query(sql)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %v", err)
	}

	// Process the result
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("error reading query result: %v", err)
		}

		// Extract the time group, dimension, and the aggregated value
		timeGroup, ok := row["timeGroup"].(Timestamp)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected value type for time group")
		}

		dimensionValue := row["dimension"]
		aggValue, ok := row["value"].(float64)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected value type for aggregation result")
		}

		// Initialize map for this timestamp if not already present
		if _, exists := result[timeGroup]; !exists {
			result[timeGroup] = make(map[any]Tuple[int64, float64])
		}

		// Add the result to the map
		result[timeGroup][dimensionValue] = Tuple[int64, float64]{Key: 1, Value: aggValue}
		total += aggValue
	}

	return result, total, nil
}

//TODO implement ToString method for logging/debugging

// ToString returns the string representation of the query
func (q *BqDatabaseQuery) ToString() string {

	return ""
}

// Unused methods for stubbing other parts of the query interface (if needed for future expansion)

func (q *BqDatabaseQuery) FindSingle(keys ...string) (Entity, error) {
	return nil, fmt.Errorf("FindSingle is not implemented in BigQuery")
}

func (q *BqDatabaseQuery) GetMap(keys ...string) (map[string]Entity, error) {
	return nil, fmt.Errorf("GetMap is not implemented in BigQuery")
}

func (q *BqDatabaseQuery) GetIDs(keys ...string) ([]string, error) {
	return nil, fmt.Errorf("GetIDs is not implemented in BigQuery")
}

func (q *BqDatabaseQuery) Delete(keys ...string) (int64, error) {
	return 0, fmt.Errorf("delete operation is not supported in BigQuery")
}

func (q *BqDatabaseQuery) SetField(field string, value any, keys ...string) (int64, error) {
	return 0, fmt.Errorf("SetField operation is not supported in BigQuery")
}

func (q *BqDatabaseQuery) SetFields(fields map[string]any, keys ...string) (int64, error) {
	return 0, fmt.Errorf("SetFields operation is not supported in BigQuery")
}

// Generic function to map a map[string]interface{} to any struct using reflection
func mapToStruct(m map[string]bigquery.Value, output interface{}) error {
	val := reflect.ValueOf(output).Elem() // Get the value the pointer points to
	typ := reflect.TypeOf(output).Elem()  // Get the type of the struct

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		structField := typ.Field(i)

		// Get the JSON tag from the struct field
		jsonTag := structField.Tag.Get("json")
		if jsonTag == "" {
			continue
		}

		// Get the value from the map using the JSON tag
		mapValue, exists := m[jsonTag]
		if !exists {
			continue // Skip if no corresponding value in the map
		}

		// Set the value based on the field type
		if field.CanSet() {
			switch field.Kind() {
			case reflect.Int, reflect.Int64:
				// Handle int and int64 types
				if v, ok := mapValue.(int64); ok {
					field.SetInt(v)
				} else if v, ok := mapValue.(int); ok {
					field.SetInt(int64(v))
				} else if v, ok := mapValue.(string); ok {
					if iv, err := strconv.ParseInt(v, 10, 64); err == nil {
						field.SetInt(iv)
					}
				}
			case reflect.String:
				// Handle string types
				if v, ok := mapValue.(string); ok {
					field.SetString(v)
				}
			// Handle other cases such as floats, booleans, etc., if needed
			case reflect.Float64:
				if v, ok := mapValue.(float64); ok {
					field.SetFloat(v)
				}
			case reflect.Bool:
				if v, ok := mapValue.(bool); ok {
					field.SetBool(v)
				}
			}
		}
	}
	return nil
}
