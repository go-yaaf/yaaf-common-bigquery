package bqdb

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
	"github.com/go-yaaf/yaaf-common/entity"
	"google.golang.org/api/iterator"
)

// BqDatabaseQuery struct for building and executing queries on BigQuery
type bqDatabaseQuery struct {
	client      *bigquery.Client
	tablePrefix string

	factory        entity.EntityFactory
	allFilters     [][]database.QueryFilter               // List of lists of AND filters
	anyFilters     [][]database.QueryFilter               // List of lists of OR filters
	ascOrders      []any                                  // List of fields for ASC order
	descOrders     []any                                  // List of fields for DESC order
	callbacks      []func(in entity.Entity) entity.Entity // List of entity transformation callback functions
	page           int                                    // Page number (for pagination)
	limit          int                                    // Page size: how many results in a page (for pagination)
	rangeField     string                                 // Field name for range filter (must be timestamp field)
	rangeFrom      entity.Timestamp                       // Start timestamp for range filter
	rangeTo        entity.Timestamp                       // End timestamp for range filter
	fieldTagToType map[string]reflect.Kind                // Holds map of Entity's field s names to their types
}

// Apply adds a callback to apply on each result entity in the query
func (s *bqDatabaseQuery) Apply(cb func(in entity.Entity) entity.Entity) database.IQuery {
	if cb != nil {
		s.callbacks = append(s.callbacks, cb)
	}
	return s
}

// Filter adds a single field filter
func (s *bqDatabaseQuery) Filter(filter database.QueryFilter) database.IQuery {
	if filter.IsActive() {
		s.allFilters = append(s.allFilters, []database.QueryFilter{filter})
	}
	return s
}

// Range adds a time frame filter on a specific time field
func (s *bqDatabaseQuery) Range(field string, from entity.Timestamp, to entity.Timestamp) database.IQuery {
	s.rangeField = field
	s.rangeFrom = from
	s.rangeTo = to
	return s
}

// MatchAll adds filters that must all be satisfied (AND)
func (s *bqDatabaseQuery) MatchAll(filters ...database.QueryFilter) database.IQuery {
	list := make([]database.QueryFilter, 0)
	for _, filter := range filters {
		if filter.IsActive() {
			list = append(list, filter)
		}
	}
	s.allFilters = append(s.allFilters, list)
	return s
}

// MatchAny adds filters where any can be satisfied (OR)
func (s *bqDatabaseQuery) MatchAny(filters ...database.QueryFilter) database.IQuery {
	list := make([]database.QueryFilter, 0)
	for _, filter := range filters {
		if filter.IsActive() {
			list = append(list, filter)
		}
	}
	s.anyFilters = append(s.anyFilters, list)
	return s
}

// Sort Add sort order by field,  expects sort parameter in the following form: field_name (Ascending) or field_name- (Descending)
func (s *bqDatabaseQuery) Sort(sort string) database.IQuery {
	if sort == "" {
		return s
	}

	// as a default, order will be ASC
	if strings.HasSuffix(sort, "-") {
		s.descOrders = append(s.descOrders, sort[0:len(sort)-1])
	} else if strings.HasSuffix(sort, "+") {
		s.ascOrders = append(s.ascOrders, sort[0:len(sort)-1])
	} else {
		s.ascOrders = append(s.ascOrders, sort)
	}
	return s
}

// Page sets the page number for pagination
func (s *bqDatabaseQuery) Page(page int) database.IQuery {
	s.page = page
	return s
}

// Limit sets the page size limit for pagination
func (s *bqDatabaseQuery) Limit(limit int) database.IQuery {
	s.limit = limit
	return s
}

// List executes a query to get a list of entities by their IDs
func (s *bqDatabaseQuery) List(entityIDs []string, keys ...string) ([]entity.Entity, error) {
	// Stub for compatibility with the interface, BigQuery doesn’t support direct ID-based fetching like traditional DBs
	return nil, fmt.Errorf("list by IDs is not supported in BigQuery")
}

// Find executes the query based on the criteria, order, and pagination
func (s *bqDatabaseQuery) Find(keys ...string) (out []entity.Entity, total int64, err error) {
	ctx := context.Background()

	// Build the SQL query from filters, sorting, etc.
	queryString := s.buildStatement()

	// Execute the query
	query := s.client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("query execution failed: %v", err)
	}

	// Process rows and convert to Entity objects
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, 0, err
		}

		entity := s.factory()
		mapToStruct(row, entity) // Assume this function maps row data to Entity
		transformed := s.processCallbacks(entity)
		if transformed != nil {
			out = append(out, transformed)
		}
	}
	// Get the rows count
	total, err = s.Count(keys...)

	return out, total, err
}

// Select executes the query and retrieves specific fields
func (s *bqDatabaseQuery) Select(fields ...string) ([]entity.Json, error) {
	return nil, fmt.Errorf("not implemented for BigQuery")
}

// Count executes the query based on the criteria and returns only the count of matching rows
func (s *bqDatabaseQuery) Count(keys ...string) (int64, error) {
	ctx := context.Background()

	// Build the SQL query to count rows
	queryString := s.buildCountStatement("", "count")

	// Execute the query
	query := s.client.Query(queryString)
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
func (s *bqDatabaseQuery) Aggregation(field string, function database.AggFunc, keys ...string) (float64, error) {

	if !isFunctionSupported(function) {
		return 0, fmt.Errorf("function %s not supported", function)
	}

	ctx := context.Background()

	// Build SQL for aggregation
	queryString := s.buildCountStatement(field, string(function))

	// Execute the query
	query := s.client.Query(queryString)
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

// GroupAggregation performs a group-by aggregation on a specified field from a BigQuery table,
// using the provided aggregation function (e.g., SUM, COUNT, AVG). It returns a map where
// the key is the group value and the value is a tuple containing the count and the aggregation result.
// Additionally, it returns the total sum of all aggregation results and an error if one occurs.
//
// Parameters:
//   - field: The name of the field on which the aggregation function will be applied (e.g., SUM of `bytes_to_srv`).
//   - function: The aggregation function to be used, defined by the `AggFunc` type (e.g., SUM, COUNT, AVG).
//   - keys: Optional additional keys used for grouping. If provided, the first key will be used as the group field
//     instead of the aggregation field.
//
// Returns:
//   - map[any]entity.Tuple[int64, float64]: A map where each key is the grouped field (e.g., src_ip) and the value
//     is a tuple containing the count and the aggregated value (e.g., total bytes).
//   - float64: The total sum of the aggregation result across all groups.
//   - error: An error if the query fails or the result processing encounters issues.
//
// Example Usage:
//   - Grouping by a source IP (src_ip) to get the total bytes sent to a server, with SUM aggregation on `bytes_to_srv`.
func (s *bqDatabaseQuery) GroupAggregation(field string, function database.AggFunc, keys ...string) (map[any]entity.Tuple[int64, float64], float64, error) {

	// Check if the function is supported
	if !isFunctionSupported(function) {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}

	result := make(map[any]entity.Tuple[int64, float64])
	total := float64(0)

	// Build the SQL query for BigQuery
	groupField := field

	if len(keys) > 0 {
		groupField = keys[0]
	}
	aggField := field // Field to perform the aggregation on

	// If the function is COUNT, we'll count the number of rows, otherwise, we'll perform the function on the field
	selectField := "*"
	if database.AggFunc(strings.ToLower(string(function))) != database.COUNT {
		selectField = fmt.Sprintf("CAST(`%s` AS FLOAT64)", aggField)
	}

	tableName := s.tableName()
	where := s.buildCriteria()

	sql := fmt.Sprintf("SELECT %s(%s) as value, `%s` as groupField FROM `%s` %s GROUP BY groupField", function, selectField, groupField, tableName, where)

	// Execute the query
	ctx := context.Background()
	query := s.client.Query(sql)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %v", err)
	}

	var castToFloat64 func(bigquery.Value) float64
	if database.AggFunc(strings.ToLower(string(function))) == database.COUNT {
		castToFloat64 = func(v bigquery.Value) float64 {
			return float64(v.(int64))
		}
	} else {
		castToFloat64 = func(v bigquery.Value) float64 {
			return v.(float64)
		}
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
		aggValue := castToFloat64(row["value"])

		// Add the result to the map
		result[groupValue] = entity.Tuple[int64, float64]{Key: 1, Value: aggValue}
		total += aggValue
	}

	return result, total, nil
}

// GroupCount Execute the query based on the criteria, grouped by field and return count per group
func (s *bqDatabaseQuery) GroupCount(field string, keys ...string) (map[any]int64, int64, error) {
	result := make(map[any]int64)
	total := int64(0)

	// Build the SQL query for grouping by the field and counting occurrences
	tableName := s.tableName()
	where := s.buildCriteria()
	sql := fmt.Sprintf("SELECT COUNT(*) as cnt, %s as groupField FROM `%s`  %s GROUP BY groupField", field, tableName, where)

	// Execute the query
	ctx := context.Background()
	query := s.client.Query(sql)
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
func (s *bqDatabaseQuery) Histogram(field string, function database.AggFunc, timeField string, interval time.Duration, keys ...string) (map[entity.Timestamp]entity.Tuple[int64, float64], float64, error) {

	// Check if the function is supported
	if !isFunctionSupported(function) {
		return nil, 0, fmt.Errorf("function %s not supported", function)
	}

	result := make(map[entity.Timestamp]entity.Tuple[int64, float64])
	total := float64(0)

	// Determine the interval format for BigQuery's date_trunc function
	intervalFormat := s.calculateDatePart(interval)

	// Build the SQL query for histogram aggregation
	tableName := s.tableName()
	where := s.buildCriteria()

	// If the function is COUNT, we'll count the number of rows, otherwise, we'll perform the function on the field
	selectField := "*"
	if database.AggFunc(strings.ToLower(string(function))) != database.COUNT {
		selectField = fmt.Sprintf("CAST(`%s` AS FLOAT64)", field)
	}
	var castToFloat64 func(bigquery.Value) float64
	if database.AggFunc(strings.ToLower(string(function))) == database.COUNT {
		castToFloat64 = func(v bigquery.Value) float64 {
			return float64(v.(int64))
		}
	} else {
		castToFloat64 = func(v bigquery.Value) float64 {
			return v.(float64)
		}
	}
	sql := fmt.Sprintf("SELECT %s(%s) as value, DATE_TRUNC(TIMESTAMP_MILLIS(%s), %s) as timeGroup FROM `%s` %s GROUP BY timeGroup ORDER BY timeGroup",
		function, selectField, timeField, intervalFormat, tableName, where)

	// Execute the query
	ctx := context.Background()
	query := s.client.Query(sql)
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
		timeGroup, ok := row["timeGroup"].(time.Time)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected value type for time group")
		}
		ts := entity.Timestamp(timeGroup.UnixNano() / int64(time.Millisecond))
		aggValue := castToFloat64(row["value"])

		result[ts] = entity.Tuple[int64, float64]{Key: 1, Value: aggValue}
		total += aggValue
	}

	return result, total, nil
}

// Histogram2D returns a two-dimensional time series data points based on the time field and an additional dimension
// The supported aggregation functions are: count, avg, sum, min, max
func (s *bqDatabaseQuery) Histogram2D(field string, function database.AggFunc, dim string, timeField string, interval time.Duration, keys ...string) (map[entity.Timestamp]map[any]entity.Tuple[int64, float64], float64, error) {
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

	result := make(map[entity.Timestamp]map[any]entity.Tuple[int64, float64])
	total := float64(0)

	// Determine the interval format for BigQuery's date_trunc function
	intervalFormat := s.calculateDatePart(interval)

	// Build the SQL query for 2D histogram aggregation
	tableName := s.factory().TABLE()
	sql := fmt.Sprintf(`SELECT %s(%s) as value, DATE_TRUNC(%s, INTERVAL %s) as timeGroup, %s as dimension FROM %s.%s GROUP BY timeGroup, dimension ORDER BY timeGroup`,
		aggFunc, field, timeField, intervalFormat, dim, s.tablePrefix, tableName)

	// Execute the query
	ctx := context.Background()
	query := s.client.Query(sql)
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
		timeGroup, ok := row["timeGroup"].(entity.Timestamp)
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
			result[timeGroup] = make(map[any]entity.Tuple[int64, float64])
		}

		// Add the result to the map
		result[timeGroup][dimensionValue] = entity.Tuple[int64, float64]{Key: 1, Value: aggValue}
		total += aggValue
	}

	return result, total, nil
}

//TODO implement ToString method for logging/debugging

// ToString returns the string representation of the query
func (s *bqDatabaseQuery) ToString() string {
	return ""
}

// Unused methods for stubbing other parts of the query interface (if needed for future expansion)

func (s *bqDatabaseQuery) FindSingle(keys ...string) (entity.Entity, error) {
	return nil, fmt.Errorf("FindSingle is not implemented in BigQuery")
}

func (s *bqDatabaseQuery) GetMap(keys ...string) (map[string]entity.Entity, error) {
	return nil, fmt.Errorf("GetMap is not implemented in BigQuery")
}

func (s *bqDatabaseQuery) GetIDs(keys ...string) ([]string, error) {
	return nil, fmt.Errorf("GetIDs is not implemented in BigQuery")
}

func (s *bqDatabaseQuery) Delete(keys ...string) (int64, error) {
	return 0, fmt.Errorf("delete operation is not supported in BigQuery")
}

func (s *bqDatabaseQuery) SetField(field string, value any, keys ...string) (int64, error) {
	return 0, fmt.Errorf("SetField operation is not supported in BigQuery")
}

func (s *bqDatabaseQuery) SetFields(fields map[string]any, keys ...string) (int64, error) {
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
