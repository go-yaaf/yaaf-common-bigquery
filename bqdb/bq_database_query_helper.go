package bqdb

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
)

var functions = map[database.AggFunc]string{
	database.COUNT: "COUNT",
	database.SUM:   "SUM",
	database.AVG:   "AVG",
	database.MIN:   "MIN",
	database.MAX:   "MAX",
}

func (s *bqDatabaseQuery) tableName() string {
	return fmt.Sprintf("%s.%s", s.tablePrefix, s.factory().TABLE())
}

// Build the SQL query string based on filters, sorting, and pagination
func (s *bqDatabaseQuery) buildStatement() string {

	// Build the SQL select
	tblName := s.tableName()

	// Build the SQL select
	// Build the WHERE clause
	where := s.buildCriteria()
	order := s.buildOrder()
	limit := s.buildLimit()

	query := fmt.Sprintf("SELECT * FROM `%s` %s %s %s ", tblName, where, order, limit)

	return query
}

// Build the SQL query string based on filters, sorting, and pagination for analytic query
func (s *bqDatabaseQuery) buildStatementAnalytic() (query string) {

	// Build the SQL select
	tblName := s.tableName()

	// Build the WHERE clause
	where := s.buildCriteria()
	order := s.buildOrder()
	limit := s.buildLimit()

	// build SELECT part
	selectPart := strings.Join(s.aggFuncs, ",")
	if counts := strings.Join(s.buildCountPart(), ","); len(counts) > 0 {
		selectPart = strings.Join([]string{selectPart, counts}, ",")
	}

	//prepare groupBy part
	groupBy := make([]string, 0, len(s.groupBys))
	//go over group by part
	for _, gb := range s.groupBys {
		fn := getBQTimePeriodSQLOrOriginalFieldName(gb.bqTag, gb.timePeriod)
		selectPart = strings.Join([]string{selectPart, fmt.Sprintf("%s AS %s ", fn, gb.dbColumnAlias)}, ",")
		groupBy = append(groupBy, gb.dbColumnAlias)
	}
	//correct for initially empty selectPart
	if selectPart[0] == ',' {
		selectPart = selectPart[1:]
	}
	if len(groupBy) == 0 {
		query = fmt.Sprintf("SELECT %s FROM `%s` %s %s %s ", selectPart, tblName, where, order, limit)
	} else {
		groupBysList := strings.Join(groupBy, ",")
		query = fmt.Sprintf("SELECT %s FROM `%s` %s GROUP BY %s %s %s ", selectPart, tblName, where, groupBysList, order, limit)
	}

	return query
}

// Build order clause based on the query data
func (s *bqDatabaseQuery) buildOrder() string {

	l := len(s.ascOrders) + len(s.descOrders)
	if l == 0 {
		return ""
	}

	fields := make([]string, 0, l)
	for _, field := range s.ascOrders {
		fields = append(fields, fmt.Sprintf(" %s ASC", field.(string)))
	}

	for _, field := range s.descOrders {
		fields = append(fields, fmt.Sprintf(" %s DESC", field.(string)))
	}

	order := fmt.Sprintf("ORDER BY %s", strings.Join(fields, " , "))
	return order
}

// Build limit clause for pagination
func (s *bqDatabaseQuery) buildLimit() string {
	// Calculate limit and offset from page number and page size (limit)
	var offset int
	if s.limit > 0 {
		if s.page < 2 {
			offset = 0
			return fmt.Sprintf(`LIMIT %d`, s.limit)
		} else {
			offset = (s.page - 1) * s.limit
			return fmt.Sprintf(`LIMIT %d OFFSET %d`, s.limit, offset)
		}
	} else {
		return ""
	}
}

// Build postgres SQL statement with sql arguments based on the query data
func (s *bqDatabaseQuery) buildCriteria() (where string) {
	parts := make([]string, 0)

	// Initialize match all (AND) conditions
	for _, list := range s.allFilters {
		for _, fq := range list {
			part := s.buildFilter(fq)
			if len(part) > 0 {
				parts = append(parts, part)
			}
		}
	}

	// Initialize match any (one of, OR) conditions
	for _, list := range s.anyFilters {
		orParts := make([]string, 0)
		for _, fq := range list {
			part := s.buildFilter(fq)
			if len(part) > 0 {
				orParts = append(orParts, part)
			}
		}

		if len(orParts) > 0 {
			orConditions := fmt.Sprintf("(%s)", strings.Join(orParts, " OR "))
			parts = append(parts, orConditions)
		}
	}

	// If range is defined, add it to the filters
	if len(s.rangeField) > 0 {
		rangeFilter := []database.QueryFilter{database.F(s.rangeField).Between(s.rangeFrom, s.rangeTo)}
		part := s.buildFilter(rangeFilter[0])
		parts = append(parts, part)
	}

	if len(parts) > 0 {
		where = fmt.Sprintf("WHERE %s", strings.Join(parts, " AND "))
	}

	return
}

// Build BigQuery SQL count statement with sql arguments based on the query data
// supported aggregations: count, sum, avg, min, max
func (s *bqDatabaseQuery) buildCountStatement(field, function string) (SQL string) {

	// Build the SQL select
	tblName := s.tableName()

	// Build the WHERE clause
	where := s.buildCriteria()

	aggr := "*"
	if function == "count" {
		SQL = fmt.Sprintf("SELECT count(%s) as aggr FROM `%s` %s", aggr, tblName, where)
	} else {
		SQL = fmt.Sprintf("SELECT cast( %s(%s) as FLOAT64) as aggr FROM `%s` %s", function, field, tblName, where)
	}

	return
}

// Build the WHERE clause based on the query filters
func (s *bqDatabaseQuery) buildFilter(qf database.QueryFilter) (sqlPart string) {

	// Ignore empty values
	if len(qf.GetValues()) == 0 {
		return ""
	}

	// Determine the field name and extract operator
	fieldName := qf.GetField()
	values := qf.GetValues()
	operator := qf.GetOperator()

	// Get the type of the field from the map
	fieldType, exists := s.bqFieldInfo[fieldName]
	if !exists {
		// Handle the case where the field is not found in the map
		return fmt.Sprintf("Unknown field: %s", fieldName)
	}

	// Helper function to handle formatting based on the field type
	formatValue := func(value interface{}) string {
		if fieldType.goType == reflect.String {
			return fmt.Sprintf("'%v'", value) // Use quotes for strings
		} else {
			return fmt.Sprintf("%v", value) // No quotes for numeric types
		}
	}

	switch operator {
	case database.Eq:
		sqlPart = fmt.Sprintf("%s = %s", fieldName, formatValue(values[0]))
	case database.Like:
		sqlPart = fmt.Sprintf("%s LIKE %s", fieldName, formatValue(values[0]))
	case database.Gt:
		sqlPart = fmt.Sprintf("%s > %s", fieldName, formatValue(values[0]))
	case database.Lt:
		sqlPart = fmt.Sprintf("%s < %s", fieldName, formatValue(values[0]))
	case database.Gte:
		sqlPart = fmt.Sprintf("%s >= %s", fieldName, formatValue(values[0]))
	case database.Lte:
		sqlPart = fmt.Sprintf("%s <= %s", fieldName, formatValue(values[0]))
	case database.Between:
		sqlPart = fmt.Sprintf("%s BETWEEN %s AND %s", fieldName, formatValue(values[0]), formatValue(values[1]))
	case database.In:
		sqlPart = fmt.Sprintf("%s IN (%s)", fieldName, s.buildListForFilter(qf))
	case database.NotIn:
		sqlPart = fmt.Sprintf("%s NOT IN (%s)", fieldName, s.buildListForFilter(qf))

	default:
		// Handle any other cases like Neq, Contains, etc.
		sqlPart = fmt.Sprintf("%s %s %s", fieldName, string(operator), formatValue(values[0]))
	}

	return
}

// Build NOT IN query filter
func (s *bqDatabaseQuery) buildListForFilter(qf database.QueryFilter) string {

	var items string

	for _, val := range qf.GetValues() {
		t := reflect.TypeOf(val).Kind()
		if t == reflect.Slice {
			items = formatSlice(toAnySlice(val))
		} else if t == reflect.String {
			items = fmt.Sprintf("%s,'%s' ", items, val)
		} else {
			items = fmt.Sprintf("%s,'%d' ", items, val)
		}
	}
	return items
}

// build COUNT's part of the SELECT
func (s *bqDatabaseQuery) buildCountPart() []string {
	var parts []string
	for _, entry := range s.counts {

		if entry.isUnique {
			// Add COUNT(DISTINCT fieldName) to the parts
			if fi, exists := s.bqFieldInfo[entry.bqTag]; exists {
				parts = append(parts, fmt.Sprintf("COUNT(DISTINCT %s) AS %s", entry.bqTag, s.resolveDbColumnAlias(fi, entry.bqTag+"_count")))
			}
		} else {
			// Add COUNT(*) to the parts
			parts = append(parts, fmt.Sprintf("COUNT(*) AS %s", entry.dbColumnAlias))
		}
	}
	return parts
}

func (s *bqDatabaseQuery) resolveDbColumnAlias(afm AnalyticFieldMapEntry, defaultAlias string) string {
	result := defaultAlias
	if afm.jsonTag != "" {
		result = afm.jsonTag
	}
	return result
}

// Transform the entity through the chain of callbacks
func (s *bqDatabaseQuery) processCallbacks(in entity.Entity) (out entity.Entity) {
	if len(s.callbacks) == 0 {
		out = in
		return
	}

	tmp := in
	for _, cb := range s.callbacks {
		out = cb(tmp)
		if out == nil {
			return nil
		} else {
			tmp = out
		}
	}
	return
}

// calculateDatePart converts the Go time.Duration into a valid BigQuery interval format
func (s *bqDatabaseQuery) calculateDatePart(interval time.Duration) string {
	switch interval {
	case time.Minute:
		return "MINUTE"
	case time.Hour:
		return "HOUR"
	case time.Hour * 24:
		return "DAY"
	case time.Hour * 24 * 7:
		return "WEEK"
	case time.Hour * 24 * 30:
		return "MONTH"
	default:
		return "DAY" // Default to daily intervals if not specified
	}
}

// Function to build map of field names and their types
func buildFieldsTypesMap(entity entity.Entity) map[string]reflect.Kind {

	fieldMap := make(map[string]reflect.Kind)

	// Get the type of the struct
	t := reflect.TypeOf(entity).Elem()

	// Loop over the fields in the struct
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if jsonTag := field.Tag.Get("json"); jsonTag != "" && jsonTag != "-" {
			// Store the JSON tag and the field type in the map
			fieldMap[jsonTag] = field.Type.Kind()
		} else {
			// JSON tag for a field is not specified, try to use "bq" tag instead
			if bqTag := field.Tag.Get("bq"); bqTag != "" {
				fieldMap[bqTag] = field.Type.Kind()
			}
		}
	}
	return fieldMap
}

func formatSlice(items []any) string {
	var result []string

	for _, item := range items {
		if reflect.TypeOf(item).Kind() == reflect.String {
			result = append(result, fmt.Sprintf("'%v'", item))
		} else {
			result = append(result, fmt.Sprintf("%v", item))
		}
	}

	// Join all items with commas and wrap the result in parentheses
	return fmt.Sprintf(" %s ", strings.Join(result, ","))
}

// Convert any slice to a slice of []any using reflection
func toAnySlice(val any) []any {
	valValue := reflect.ValueOf(val)
	if valValue.Kind() != reflect.Slice {
		return nil
	}

	anySlice := make([]any, valValue.Len())
	for i := 0; i < valValue.Len(); i++ {
		anySlice[i] = valValue.Index(i).Interface()
	}
	return anySlice
}

func isFunctionSupported(f database.AggFunc) bool {
	_, ok := functions[database.AggFunc(strings.ToLower(string(f)))]
	return ok
}

type AnalyticFieldMapEntry struct {
	goType  reflect.Kind
	dbType  string
	jsonTag string
}

// AnalyticsFieldMap holds map of fields names of an conrete type (that is of EntitySharded) instance
// to AnalyticsFieldMapEntry
type AnalyticFielsdMap map[string]AnalyticFieldMapEntry

// Function to build map of field names and their types
func buildAnalyticFieldsdMap(entity entity.Entity) AnalyticFielsdMap {

	fieldMap := make(AnalyticFielsdMap)

	// Get the type of the struct
	t := reflect.TypeOf(entity).Elem()

	// Loop over the fields in the struct
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Get the BQ tag
		bqTag := field.Tag.Get("bq")
		if bqTag == "" {
			continue
		}
		jsonTag := field.Tag.Get("json")

		// get DB type to cast to, if specified
		bqCastTo := field.Tag.Get("castTo")
		fieldMap[bqTag] = AnalyticFieldMapEntry{
			dbType:  bqCastTo,
			goType:  field.Type.Kind(),
			jsonTag: jsonTag,
		}
	}
	return fieldMap
}

// Function to return the corresponding BigQuery SQL transformation
func getBQTimePeriodSQLOrOriginalFieldName(fieldName string, period entity.TimePeriodCode) string {
	// Milliseconds in each time unit
	const (
		MinuteMillis = 60 * 1000
		HourMillis   = 60 * MinuteMillis
		DayMillis    = 24 * HourMillis
		WeekMillis   = 7 * DayMillis
	)
	switch period {
	case entity.TimePeriodCodes.MINUTE:
		return fmt.Sprintf("start_time - MOD(start_time, %d)", MinuteMillis)
	case entity.TimePeriodCodes.HOUR:
		return fmt.Sprintf("start_time - MOD(start_time, %d)", HourMillis)
	case entity.TimePeriodCodes.DAY:
		return fmt.Sprintf("start_time - MOD(start_time, %d)", DayMillis)
	case entity.TimePeriodCodes.WEEK:
		return fmt.Sprintf("start_time - MOD(start_time, %d)", WeekMillis)
	//case entity.TimePeriodCodes.MONTH:
	// MONTH calculation is more complex due to varying month lengths; consider using TIMESTAMP_TRUNC for month.
	///	return "TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(start_time), MONTH)"

	default:
		//TODO ignore error of unknown TimePeriod code and return original field name
		return fieldName
	}
}
