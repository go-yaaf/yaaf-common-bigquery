# Go-YAAF Google BigQuery Middleware

![Project status](https://img.shields.io/badge/version-1.2-green.svg)
[![Build](https://github.com/go-yaaf/yaaf-common-bigquery/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-bigquery/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/go-yaaf/yaaf-common-bigquery/badge.svg?branch=main&service=github)](https://coveralls.io/github/go-yaaf/yaaf-common-bigquery?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-yaaf/yaaf-common-bigquery)](https://goreportcard.com/report/github.com/go-yaaf/yaaf-common-bigquery)
[![GoDoc](https://godoc.org/github.com/go-yaaf/yaaf-common-bigquery?status.svg)](https://pkg.go.dev/github.com/go-yaaf/yaaf-common-bigquery)
![License](https://img.shields.io/dub/l/vibe-d.svg)

## Overview
This library is a BigQuery adapter for the `go-yaaf/yaaf-common` database interface. It provides a convenient and fluent API for interacting with Google BigQuery, allowing developers to easily build and execute complex queries, perform bulk data operations, and manage database schema.

## Features

- **Fluent Query Builder**: Construct complex SQL queries with a simple, chainable API.
- **Rich Filtering**: Supports a wide range of filtering options, including `Eq`, `Like`, `In`, `NotIn`, `Gt`, `Gte`, `Lt`, `Lte`, and `Range`.
- **Powerful Aggregations**: Perform aggregations like `Count`, `Sum`, `Avg`, `Max`, and `Min` with ease.
- **Group-By Support**: Execute `GROUP BY` queries to aggregate data based on specific fields.
- **Bulk Operations**: Efficiently insert large volumes of data using `BulkInsert`.
- **Raw SQL Execution**: Run raw SQL queries for both DML and `SELECT` statements.
- **DDL Execution**: Execute Data Definition Language (DDL) statements to manage table schemas.

## Installation

To install the library, use the following `go get` command:

```bash
go get github.com/go-yaaf/yaaf-common-bigquery
```

## Getting Started

To begin, create a new BigQuery database instance using `bqdb.NewBqDatabase`. The function requires a URI in the format `bq://project-id:dataset-id`.

```go
package main

import (
	"log"
	"github.com/go-yaaf/yaaf-common-bigquery/bqdb"
	"github.com/go-yaaf/yaaf-common/database"
)

func main() {
	// Replace with your project ID and dataset ID
	uri := "bq://your-project-id:your-dataset-id"

	// Create a new BigQuery database instance
	db, err := bqdb.NewBqDatabase(uri)
	if err != nil {
		log.Fatalf("Failed to connect to BigQuery: %v", err)
	}
	defer db.Close()

	// You can now use the db object to interact with BigQuery
}
```

## Querying Data

The library provides a fluent query builder that makes it easy to construct and execute queries.

### Basic Query with Filters

To perform a basic query with a filter, use the `Filter` method. The following example retrieves records where the `dst_ip` is "8.8.8.8".

```go
// Assuming 'db' is an initialized BqDatabase object
// and 'NewFlowRecordEntity' is your entity factory
flowRecords, _, err := db.Query(NewFlowRecordEntity("your-stream-id")).
	Filter(database.F("dst_ip").Eq("8.8.8.8")).
	Limit(100).
	Page(0).
	Find()

if err != nil {
	log.Fatalf("Query failed: %v", err)
}

// Process the retrieved flowRecords
```

### Advanced Filtering

You can chain multiple `Filter` calls and use various operators to create more complex queries.

- **LIKE**: For pattern matching.
- **IN`/`NOT IN**: For checking against a list of values.
- **RANGE**: For filtering based on a time range.

```go
// Example of a complex query
flowRecords, _, err := db.Query(NewFlowRecordEntity("your-stream-id")).
	Filter(database.F("src_ip").Like("10.245.137.%")).
	Filter(database.F("dst_ip").NotIn("1.1.1.1", "8.8.8.8")).
	Filter(database.F("protocol").In("TCP", "UDP")).
	Range("start_time", fromTimestamp, toTimestamp).
	Sort("bytes_to_srv", "ASC").
	Limit(1000).
	Page(0).
	Find()
```

## Aggregations

The library supports various aggregation functions.

### Simple Aggregations

You can perform simple aggregations like `Count`, `Sum`, `Avg`, and `Max`.

```go
// Count records matching the filters
count, err := db.Query(NewFlowRecordEntity("your-stream-id")).
	Filter(database.F("src_ip").Like("10.245.137.%")).
	Count()

// Calculate the sum of 'bytes_to_srv'
totalBytes, err := db.Query(NewFlowRecordEntity("your-stream-id")).
	Filter(database.F("src_ip").Like("10.245.137.%")).
	Aggregation("bytes_to_srv", "sum")
```

### Group-By Aggregations

You can also perform `GROUP BY` aggregations.

```go
// Group by 'src_ip' and count records in each group
result, total, err := db.Query(NewFlowRecordEntity("your-stream-id")).
	Filter(database.F("src_ip").Like("10.245.137.%")).
	GroupCount("src_ip")

// Group by 'src_ip' and calculate the sum of 'bytes_to_srv' for each group
result, total, err := db.Query(NewFlowRecordEntity("your-stream-id")).
	Filter(database.F("src_ip").Like("10.245.137.%")).
	GroupAggregation("bytes_to_srv", "sum", "src_ip")
```

## Bulk Insert

To insert a large number of records efficiently, use the `BulkInsert` method.

```go
// entities is a slice of your entity.Entity objects
insertedCount, err := db.BulkInsert(entities)
if err != nil {
	log.Fatalf("Bulk insert failed: %v", err)
}
log.Printf("Successfully inserted %d records", insertedCount)
```

## Executing Raw SQL

For more complex scenarios, you can execute raw SQL queries.

### DML Statements

Use `ExecuteSQL` for DML statements like `DELETE`, `UPDATE`, and `INSERT`. The `{table_name}` placeholder is automatically replaced with the fully qualified table name.

```go
sql := "DELETE FROM {table_name} WHERE status = @status"
affectedRows, err := db.ExecuteSQL(sql, "your-table-name", "inactive")
```

### SELECT Statements

Use `ExecuteQuery` for `SELECT` statements. The results are returned as a slice of `entity.Json`.

```go
sql := "SELECT src_ip, COUNT(*) as count FROM {table_name} GROUP BY src_ip"
results, err := db.ExecuteQuery("", sql, "your-table-name")
```

## Executing DDL

You can execute DDL statements to create or modify tables using `ExecuteDDL`.

```go
createTableSQL := `
CREATE TABLE %s (
  id INT64 NOT NULL,
  name STRING,
  timestamp TIMESTAMP
)
`
ddl := map[string][]string{
	"shardKey":  {"my-shard"},
	"tableName": {"my-table"},
	"sql":       {createTableSQL},
}

err := db.ExecuteDDL(ddl)
if err != nil {
	log.Fatalf("DDL execution failed: %v", err)
}
```

## Running Tests

The library includes a comprehensive test suite. To run the tests, you'll need to set up a BigQuery project and dataset and provide the necessary credentials. The tests are skipped in CI environments by default.

```bash
# Run tests
go test ./...
```
