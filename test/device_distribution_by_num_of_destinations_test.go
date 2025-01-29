package test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
)

type DeviceDestinationDistribution struct {
	BaseAnalyticEntity
	StreamId   string `json:"-"`
	TimePoint  int64  `json:"-"          bq:"start_time"`
	DeviceId   string `json:"deviceId"   bq:"device_id"`
	NumOfDstIp int64  `json:"numOfDstIp" bq:"dst_ip"`
}

func (f *DeviceDestinationDistribution) KEY() string {
	return f.StreamId
}

func (f *DeviceDestinationDistribution) TABLE() string {
	return fmt.Sprintf("flow-data-%s", f.KEY())
}

func NewDeviceDestinationDistribution(shardKey string) entity.EntityFactory {
	return func() entity.Entity {
		shardKey := shardKey
		return &DeviceDestinationDistribution{
			StreamId: shardKey,
		}
	}
}

func Test_Device_Distribution_By_Num_Of_Destinations(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
	shardKey := "etecnic-1"
	factory := NewDeviceDestinationDistribution(shardKey)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	qa := bqdb.AdvancedQuery(factory)

	entities, err := qa.GroupBy("device_id", entity.TimePeriodCodes.UNDEFINED).
		CountUnique("dst_ip").
		Compute()

	if err != nil {
		t.Fatal(err)
	}

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(entities, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	// Print the result
	fmt.Println(string(jsonData))
	logger.Debug("count: %d", len(entities))
}
