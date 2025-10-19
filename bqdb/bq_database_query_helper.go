package bqdb

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	//resolve order's field alias
	fn := func(bqTag string) string {
		if v, ok := s.bqFieldInfo[bqTag]; ok {
			return v.jsonTag
		}
		return bqTag
	}

	fields := make([]string, 0, l)
	for _, field := range s.ascOrders {
		fields = append(fields, fmt.Sprintf(" %s ASC", fn(field.(string))))
	}

	for _, field := range s.descOrders {
		fields = append(fields, fmt.Sprintf(" %s DESC", fn(field.(string))))
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
	if len(items) > 0 {
		items = items[1:]
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

		// Get the BQ tag. Try default BQ's SDK annotation tag first
		bqTag := field.Tag.Get("bigquery")
		//then try custom "bq" tag
		if bqTag == "" {
			if bqTag = field.Tag.Get("bq"); bqTag == "" {
				continue
			}
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

// entityToProto builds a dynamic protobuf message according to msgDesc using reflection over the entity.
// Field-name precedence: bigquery tag -> bq tag -> json tag -> snake_case(field name).
// Special handling: when proto field is google.protobuf.Timestamp, accept either time.Time or epoch-millis (int/uint/float/string).
func entityToProto(msgDesc protoreflect.MessageDescriptor, e entity.Entity) (*dynamicpb.Message, error) {
	if e == nil {
		return nil, fmt.Errorf("nil entity")
	}

	// Peel pointers to reach a struct
	rv := reflect.ValueOf(e)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("nil entity pointer")
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("entityToProto expects struct, got %s", rv.Kind())
	}

	dm := dynamicpb.NewMessage(msgDesc)
	fields := msgDesc.Fields()
	rt := rv.Type()

	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		// Skip unexported fields
		if sf.PkgPath != "" {
			continue
		}
		// Explicit skip
		if tag := strings.TrimSpace(sf.Tag.Get("bigquery")); tag == "-" {
			continue
		}
		// Resolve the column (proto field) name
		col := resolveProtoColumnName(sf)
		fd := fields.ByName(protoreflect.Name(col))
		if fd == nil {
			// Not part of the target schema/descriptor; ignore gracefully
			continue
		}

		fv := rv.Field(i)
		// Handle nil pointers as NULL (skip)
		if fv.Kind() == reflect.Pointer && fv.IsNil() {
			continue
		}
		// If it’s a Timestamp message, accept time.Time or epoch millis
		if fd.Kind() == protoreflect.MessageKind && string(fd.Message().FullName()) == "google.protobuf.Timestamp" {
			ms, ok := getMillis(fv.Interface())
			if !ok {
				// treat as NULL/absent (field is NOT NULL? BigQuery will error, can see that in row index)
				continue
			}
			sec, nanos := msToSecNanos(ms)
			ts := timestamppb.New(time.Unix(sec, int64(nanos)).UTC())
			if err := ts.CheckValid(); err != nil {
				return nil, fmt.Errorf("field %s: invalid timestamp from ms=%d: %w", col, ms, err)
			}
			dm.Set(fd, protoreflect.ValueOfMessage(ts.ProtoReflect()))
			continue
		}

		// Generic scalar mapping (and bytes)
		val, ok, err := reflectToProtoValue(fd, fv)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", col, err)
		}
		if ok {
			dm.Set(fd, val)
		}
	}

	return dm, nil
}

// resolveProtoColumnName picks the outgoing column/field name by tag precedence.
func resolveProtoColumnName(sf reflect.StructField) string {
	if t := strings.TrimSpace(sf.Tag.Get("bigquery")); t != "" {
		return t
	}
	if t := strings.TrimSpace(sf.Tag.Get("bq")); t != "" {
		return t
	}
	if j := strings.TrimSpace(sf.Tag.Get("json")); j != "" {
		if p := strings.Split(j, ",")[0]; p != "" && p != "-" {
			return p
		}
	}
	return toSnakeCase(sf.Name)
}

// toSnakeCase converts CamelCase -> snake_case.
func toSnakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}

// toProtoTimestamp accepts either time.Time or epoch-millis (int/uint/float/string) and returns a timestamppb.
// ok=false means "treat as NULL/absent".
func toProtoTimestamp(v any) (*timestamppb.Timestamp, bool, error) {
	if v == nil {
		return nil, false, nil
	}
	switch x := v.(type) {
	case time.Time:
		if x.IsZero() {
			return nil, false, nil
		}
		ts := timestamppb.New(x.UTC())
		if err := ts.CheckValid(); err != nil {
			return nil, false, err
		}
		return ts, true, nil
	case *time.Time:
		if x == nil || x.IsZero() {
			return nil, false, nil
		}
		ts := timestamppb.New(x.UTC())
		if err := ts.CheckValid(); err != nil {
			return nil, false, err
		}
		return ts, true, nil
	default:
		// Attempt epoch-millis via existing helpers
		ms, err := toInt64(v)
		if err != nil {
			return nil, false, fmt.Errorf("expect time.Time or epoch-millis: %w", err)
		}
		sec := ms / 1000
		nsec := (ms % 1000) * int64(time.Millisecond)
		t := time.Unix(sec, nsec).UTC()
		ts := timestamppb.New(t)
		if err := ts.CheckValid(); err != nil {
			return nil, false, err
		}
		return ts, true, nil
	}
}

// reflectToProtoValue converts a reflect.Value into a protoreflect.Value according to fd.
// ok=false means "skip" (e.g., nil ptr). Uses the existing numeric/string/boolean coercers in this file.
func reflectToProtoValue(fd protoreflect.FieldDescriptor, fv reflect.Value) (protoreflect.Value, bool, error) {
	// Unwrap pointers
	for fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			return protoreflect.Value{}, false, nil
		}
		fv = fv.Elem()
	}

	switch fd.Kind() {
	case protoreflect.BoolKind:
		b, err := toBool(fv.Interface())
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		return protoreflect.ValueOfBool(b), true, nil

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		i, err := toInt64(fv.Interface())
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		return protoreflect.ValueOfInt32(int32(i)), true, nil

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		i, err := toInt64(fv.Interface())
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		return protoreflect.ValueOfInt64(i), true, nil

	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		i, err := toInt64(fv.Interface())
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		return protoreflect.ValueOfUint32(uint32(i)), true, nil

	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		i, err := toInt64(fv.Interface())
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		return protoreflect.ValueOfUint64(uint64(i)), true, nil

	case protoreflect.DoubleKind, protoreflect.FloatKind:
		// If the target is TIMESTAMP-as-seconds (proto3 DOUBLE), convert ms -> seconds.
		// We can’t inspect logical types here, so we optimistically convert integers/time.Time as ms.
		switch fv.Kind() {
		case reflect.Struct:
			if fv.Type().PkgPath() == "time" && fv.Type().Name() == "Time" {
				tt := fv.Interface().(time.Time).UTC()
				secs := float64(tt.UnixNano()) / 1e9
				if fd.Kind() == protoreflect.FloatKind {
					return protoreflect.ValueOfFloat32(float32(secs)), true, nil
				}
				return protoreflect.ValueOfFloat64(secs), true, nil
			}
		}
		if isIntLike(fv.Kind()) {
			// interpret as epoch-ms and convert to seconds with integer math first
			ms, ok := getMillis(fv.Interface())
			if ok {
				sec := ms / 1000
				rem := ms % 1000
				secs := float64(sec) + float64(rem)/1000.0
				if fd.Kind() == protoreflect.FloatKind {
					return protoreflect.ValueOfFloat32(float32(secs)), true, nil
				}
				return protoreflect.ValueOfFloat64(secs), true, nil
			}
		}
		// Fallback: generic float (non-timestamp doubles)
		f, err := toFloat64(fv.Interface())
		if err != nil {
			return protoreflect.Value{}, false, err
		}
		if fd.Kind() == protoreflect.FloatKind {
			return protoreflect.ValueOfFloat32(float32(f)), true, nil
		}
		return protoreflect.ValueOfFloat64(f), true, nil

	case protoreflect.StringKind:
		return protoreflect.ValueOfString(toString(fv.Interface())), true, nil

	case protoreflect.BytesKind:
		switch fv.Kind() {
		case reflect.Slice:
			if fv.Type().Elem().Kind() == reflect.Uint8 {
				return protoreflect.ValueOfBytes(fv.Bytes()), true, nil
			}
		case reflect.String:
			return protoreflect.ValueOfBytes([]byte(fv.String())), true, nil
		}
		return protoreflect.Value{}, false, fmt.Errorf("bytes: unsupported source kind %s", fv.Kind())

	case protoreflect.MessageKind:
		// Timestamp is handled earlier; other nested messages not supported by this mapper.
		return protoreflect.Value{}, false, fmt.Errorf("unsupported message type: %s", fd.Message().FullName())

	default:
		return protoreflect.Value{}, false, fmt.Errorf("unsupported proto kind: %s", fd.Kind())
	}
}

// protoIdentSafe converts an arbitrary string into a valid protobuf identifier:
// [A-Za-z_][A-Za-z0-9_]*
func protoIdentSafe(s string) string {
	if s == "" {
		return "p_"
	}
	out := make([]rune, 0, len(s))
	for i, r := range s {
		ok := (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || (i > 0 && r >= '0' && r <= '9')
		if ok {
			out = append(out, r)
		} else {
			out = append(out, '_')
		}
	}
	// First rune must be letter or '_'
	first := out[0]
	if !((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z') || first == '_') {
		out = append([]rune{'p', '_'}, out...)
	}
	return string(out)
}

func isIntLike(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case float64:
		// JSON numbers decode to float64; require integral value
		if x > math.MaxInt64 || x < math.MinInt64 {
			return 0, fmt.Errorf("out of int64 range")
		}
		return int64(x), nil
	case json.Number:
		i, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			// try float then cast
			f, e := strconv.ParseFloat(string(x), 64)
			if e != nil {
				return 0, err
			}
			return int64(f), nil
		}
		return i, nil
	case string:
		if x == "" {
			return 0, fmt.Errorf("empty string")
		}
		i, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, err
		}
		return i, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case int:
		return float64(x), nil
	case json.Number:
		return strconv.ParseFloat(string(x), 64)
	case string:
		return strconv.ParseFloat(x, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		switch x {
		case "true", "1", "t", "yes", "y":
			return true, nil
		case "false", "0", "f", "no", "n":
			return false, nil
		}
		return false, fmt.Errorf("invalid bool string %q", x)
	case float64:
		return x != 0, nil
	case int64:
		return x != 0, nil
	case int:
		return x != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}

// msToSecNanos converts epoch milliseconds to (seconds, nanos).
func msToSecNanos(ms uint64) (sec int64, nanos int32) {
	sec = int64(ms / 1000)
	remMs := int64(ms % 1000)
	nanos = int32(remMs * 1_000_000)
	return
}

// msToMicros converts epoch milliseconds to microseconds (int64), with overflow guard.
func msToMicros(ms uint64) (int64, error) {
	const max = ^uint64(0) // max uint64
	// Bound for int64
	if ms > uint64(math.MaxInt64)/1000 {
		return 0, fmt.Errorf("timestamp overflows microseconds int64: ms=%d", ms)
	}
	return int64(ms * 1000), nil
}

// getMillis extracts epoch-ms from common numeric/time forms quickly.
func getMillis(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case *uint64:
		if x == nil {
			return 0, false
		}
		return *x, true
	case int64:
		if x < 0 {
			return uint64(x), true
		}
		return uint64(x), true
	case *int64:
		if x == nil {
			return 0, false
		}
		if *x < 0 {
			return uint64(*x), true
		}
		return uint64(*x), true
	case int, int32, uint, uint32:
		return uint64(reflect.ValueOf(v).Convert(reflect.TypeOf(uint64(0))).Uint()), true
	case time.Time:
		if x.IsZero() {
			return 0, false
		}
		return uint64(x.UnixMilli()), true
	case *time.Time:
		if x == nil || x.IsZero() {
			return 0, false
		}
		return uint64(x.UnixMilli()), true
	default:
		// Fall back to existing toInt64 if present
		i, err := toInt64(v)
		if err != nil {
			return 0, false
		}
		if i < 0 {
			return uint64(i), true
		}
		return uint64(i), true
	}
}
