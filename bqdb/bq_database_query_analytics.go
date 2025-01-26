package bqdb

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"google.golang.org/api/iterator"
)

func (s *bqDatabaseQuery) Sum(fieldName string) database.IQueryAnalytic {
	if fi, exists := s.bqFieldInfo[fieldName]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" SUM(%s) as %s ", fieldName, fi.jsonTag))
	}
	return s
}

func (s *bqDatabaseQuery) Min(fieldName string) database.IQueryAnalytic {
	if fi, exists := s.bqFieldInfo[fieldName]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" MIN(%s) as %s ", fieldName, fi.jsonTag))
	}
	return s
}

func (s *bqDatabaseQuery) Max(fieldName string) database.IQueryAnalytic {
	if fi, exists := s.bqFieldInfo[fieldName]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" MAX(%s) as %s ", fieldName, fi.jsonTag))
	}
	return s
}

func (s *bqDatabaseQuery) Avg(fieldName string) database.IQueryAnalytic {
	if fi, exists := s.bqFieldInfo[fieldName]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" AVG(%s) as %s ", fieldName, fi.jsonTag))
	}
	return s
}

func (s *bqDatabaseQuery) GroupBy(fieldName string, period entity.TimePeriodCode) database.IQueryAnalytic {
	fieldAlias := fieldName + "_grouped"
	if fi, exists := s.bqFieldInfo[fieldName]; exists {
		fieldAlias = fi.jsonTag
	}

	s.groupBys = append(s.groupBys, groupByEntry{
		fieldName:  fieldName,
		fieldAlias: fieldAlias,
		timePeriod: period,
	})
	return s
}

func (s *bqDatabaseQuery) Compute() (out []entity.Entity, err error) {

	ctx := context.Background()

	// Build the SQL query from filters, sorting, etc.
	queryString := s.buildStatementAnalytic()

	// Execute the query
	query := s.client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %v", err)
	}

	// Process rows and convert to Entity objects
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		entity := s.factory()
		mapToStruct(row, entity)
		if entity = s.processCallbacks(entity); entity != nil {
			out = append(out, entity)
		}
	}
	// Get the rows count

	return out, err
}
