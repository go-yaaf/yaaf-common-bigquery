package test

import (
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
)

type NewtworkActivityOverTime struct {
	entity.BaseEntity
	TimePoint    int64 `json:"timestamp" bq:"start_time"`
	NumOfDevices int64 `json:"value"     bq:"device_id"`
}

func TestNetworkActivityOverTime(t *testing.T) {
	shardKey := "etecnic-1"
	factory := NewDeviceConsumption(shardKey)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}
}
