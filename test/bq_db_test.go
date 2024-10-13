package test

import (
	"testing"

	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/logger"
	bigquerydb "github.com/go-yaaf/yaff-common-bigquery/bigquery"
)

func TestBqDatabase(t *testing.T) {

	//Filter(F("streamId").Eq(stream.(*Stream).Id)).
	bqdb, err := bigquerydb.NewBqDatabase("shieldiot-staging", "pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	flowRrecords, _, err := bqdb.Query(NewFlowRecordEntity).
		Filter(F("dst_ip").Eq("8.8.8.8")).
		Limit(1000).
		Page(0).
		Find()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("%v", flowRrecords)
}
