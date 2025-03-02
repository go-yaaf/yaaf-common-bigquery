package test

import (
	"fmt"
	"os"
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
)

func TestDeleteRowsFromFlow(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}

	from, _ := ConvertToTimestamp("01-01-2025 00:00")
	to, _ := ConvertToTimestamp("31-01-2025 23:59")

	factory := NewFlowRecordEntity("etecnic-1-suri")
	sql := fmt.Sprintf("delete from `{table_name}` where start_time >= %d and end_time <= %d", from, to)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")
	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	affectedRecords, err := bqdb.ExecuteSQL(sql, factory().TABLE())
	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	fmt.Printf("records deleted: %d", affectedRecords)
}
