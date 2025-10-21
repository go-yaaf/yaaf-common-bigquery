package test

import (
	"fmt"
	"log"
	"os"
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
)

func TestBQBulkInsert(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
	os.Setenv("BQ_BATCH_SIZE", "5000")
	os.Setenv("BQ_BATCH_TIMEOUT", "15")

	/* 	ddl := map[string][]string{
	   		"tn":    {"flow"},
	   		"sql":   {"select...."},
	   		"shard": {"etecnic"},
	   	}

	   	mv := ddl["mv"]

	   	fmt.Print(mv) */

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	flows := generateFlowRecords(5000)
	affected, err := bqdb.BulkInsert(flows)

	if err != nil {
		log.Printf("error when bulk insert: %s\n", err)
		t.Fail()
	}

	fmt.Printf("affected: %d records.\n", affected)
}
