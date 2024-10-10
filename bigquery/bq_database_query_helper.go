package bigquerydb

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common/database"
	"strings"
	"time"
)

// Build the SQL query string based on filters, sorting, and pagination
func (q *BqDatabaseQuery) buildSQL() string {
	selectClause := "*"
	if len(q.selectFields) > 0 {
		selectClause = strings.Join(q.selectFields, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM `your_project.dataset.table`", selectClause)

	// Add WHERE filters
	if len(q.allFilters) > 0 {
		whereClause := q.buildWhereClause()
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	// Add ORDER BY if present
	if len(q.orderBy) > 0 {
		query += fmt.Sprintf(" ORDER BY %s", strings.Join(q.orderBy, ", "))
	}

	// Add LIMIT and OFFSET for pagination
	if q.limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", q.limit)
	}
	if q.offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", q.offset)
	}

	return query
}

// Build the SQL for counting rows
func (q *BqDatabaseQuery) buildSQLForCount() string {
	query := "SELECT COUNT(*) FROM `your_project.dataset.table`"

	// Add WHERE filters
	if len(q.allFilters) > 0 {
		whereClause := q.buildWhereClause()
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	return query
}

// Build the WHERE clause based on the query filters
func (q *BqDatabaseQuery) buildWhereClause() string {
	var conditions []string
	for _, filter := range q.allFilters {
		field := filter.GetField()
		operator := filter.GetOperator()
		values := filter.GetValues()

		switch operator {
		case database.Eq:
			conditions = append(conditions, fmt.Sprintf("%s = '%v'", field, values[0]))
		case database.Like:
			conditions = append(conditions, fmt.Sprintf("%s LIKE '%v'", field, values[0]))
		case database.Gt:
			conditions = append(conditions, fmt.Sprintf("%s > '%v'", field, values[0]))
		case database.Lt:
			conditions = append(conditions, fmt.Sprintf("%s < '%v'", field, values[0]))
		case database.Gte:
			conditions = append(conditions, fmt.Sprintf("%s >= '%v'", field, values[0]))
		case database.Lte:
			conditions = append(conditions, fmt.Sprintf("%s <= '%v'", field, values[0]))
		case database.Between:
			conditions = append(conditions, fmt.Sprintf("%s BETWEEN '%v' AND '%v'", field, values[0], values[1]))
		case database.In:
			inValues := make([]string, len(values))
			for i, v := range values {
				inValues[i] = fmt.Sprintf("'%v'", v)
			}
			conditions = append(conditions, fmt.Sprintf("%s IN (%s)", field, strings.Join(inValues, ", ")))
		case database.NotIn:
			notInValues := make([]string, len(values))
			for i, v := range values {
				notInValues[i] = fmt.Sprintf("'%v'", v)
			}
			conditions = append(conditions, fmt.Sprintf("%s NOT IN (%s)", field, strings.Join(notInValues, ", ")))
		default:
			// Handle any other cases like Neq, Contains, etc.
			conditions = append(conditions, fmt.Sprintf("%s %s '%v'", field, string(operator), values[0]))
		}
	}

	return strings.Join(conditions, " AND ")
}

// Build a series of OR conditions (for MatchAny)
func (q *BqDatabaseQuery) buildFilterConditions(filters []database.QueryFilter, joinOperator string) string {
	var conditions []string
	for _, filter := range filters {
		field := filter.GetField()
		operator := filter.GetOperator()
		values := filter.GetValues()

		switch operator {
		case database.Eq:
			conditions = append(conditions, fmt.Sprintf("%s = '%v'", field, values[0]))
		case database.Like:
			conditions = append(conditions, fmt.Sprintf("%s LIKE '%v'", field, values[0]))
		case database.Gt:
			conditions = append(conditions, fmt.Sprintf("%s > '%v'", field, values[0]))
		case database.Lt:
			conditions = append(conditions, fmt.Sprintf("%s < '%v'", field, values[0]))
		case database.Between:
			conditions = append(conditions, fmt.Sprintf("%s BETWEEN '%v' AND '%v'", field, values[0], values[1]))
		default:
			// Default case for other operators
			conditions = append(conditions, fmt.Sprintf("%s %s '%v'", field, string(operator), values[0]))
		}
	}

	return strings.Join(conditions, joinOperator)
}

// calculateDatePart converts the Go time.Duration into a valid BigQuery interval format
func (q *BqDatabaseQuery) calculateDatePart(interval time.Duration) string {
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
