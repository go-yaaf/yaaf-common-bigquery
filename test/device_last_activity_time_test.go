package test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
)

type DeviceLastActivityTime struct {
	entity.BaseEntity
	StreamId  string `json:"-"`
	TimePoint int64  `json:"timestamp" bq:"end_time"`
	DeviceId  string `json:"value"     bq:"device_id"`
}

func (f *DeviceLastActivityTime) KEY() string {
	return f.StreamId
}

func (f *DeviceLastActivityTime) TABLE() string {
	return fmt.Sprintf("flow-data-%s", f.KEY())
}

func NewDeviceLastActivityTime(shardKey string) entity.EntityFactory {
	return func() entity.Entity {
		shardKey := shardKey
		return &DeviceLastActivityTime{
			StreamId: shardKey,
		}
	}
}

func TestDeviceLastActivityTime(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
	shardKey := "etecnic-1"
	factory := NewDeviceLastActivityTime(shardKey)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	qa := bqdb.AdvancedQuery(factory)

	qa.Filter(F("end_time").Between(1737298336679, 1739958520951))

	entities, err := qa.GroupBy("device_id", entity.TimePeriodCodes.UNDEFINED).
		Max("end_time").
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
}
