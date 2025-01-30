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

func (s *bqDatabaseQuery) Sum(bqTag string) database.IAnalyticQuery {
	if fi, exists := s.bqFieldInfo[bqTag]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" SUM(%s) as %s ", bqTag, fi.jsonTag))
	}
	return s
}

func (s *bqDatabaseQuery) Min(bqTag string) database.IAnalyticQuery {
	if fi, exists := s.bqFieldInfo[bqTag]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" MIN(%s) as %s ", bqTag, fi.jsonTag))
	}
	return s
}

func (s *bqDatabaseQuery) Max(bqTag string) database.IAnalyticQuery {
	if fi, exists := s.bqFieldInfo[bqTag]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" MAX(%s) as %s ", bqTag, fi.jsonTag))
	}
	return s
}

func (s *bqDatabaseQuery) Avg(bqTag string) database.IAnalyticQuery {
	if fi, exists := s.bqFieldInfo[bqTag]; exists {
		s.aggFuncs = append(s.aggFuncs, fmt.Sprintf(" AVG(%s) as %s ", bqTag, s.resolveDbColumnAlias(fi, bqTag+"_avg")))
	}
	return s
}

func (s *bqDatabaseQuery) CountAll(bqTag string) database.IAnalyticQuery {
	if fi, exists := s.bqFieldInfo[bqTag]; exists {
		s.counts = append(s.counts, countEnrty{
			bqTag:         "*",
			dbColumnAlias: s.resolveDbColumnAlias(fi, "count"),
			isUnique:      false,
		})
	}
	return s
}
func (s *bqDatabaseQuery) CountUnique(bqTag string) database.IAnalyticQuery {

	if fi, exists := s.bqFieldInfo[bqTag]; exists {
		s.counts = append(s.counts, countEnrty{
			bqTag:         bqTag,
			dbColumnAlias: s.resolveDbColumnAlias(fi, bqTag+"_count"),
			isUnique:      true,
		})
	}
	return s
}

func (s *bqDatabaseQuery) GroupBy(bqTag string, period entity.TimePeriodCode) database.IAnalyticQuery {

	if fi, exists := s.bqFieldInfo[bqTag]; exists {
		s.groupBys = append(s.groupBys, groupByEntry{
			bqTag:         bqTag,
			dbColumnAlias: s.resolveDbColumnAlias(fi, bqTag+"_grouped"),
			timePeriod:    period,
		})
	}
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
	s.lastSqlStatement = queryString
	return out, err
}
