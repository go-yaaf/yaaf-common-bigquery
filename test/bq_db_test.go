package test

import (
	"testing"

	bigquerydb "github.com/go-yaaf/yaff-common-bigquery/bigquery"
)

func TestBqDatabase(t *testing.T) {

	bqdb, err := bigquerydb.NewBqDatabase("shieldiot-staging", "pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	bqdb.Query(NewFlowRecordEntity).Find()
}
