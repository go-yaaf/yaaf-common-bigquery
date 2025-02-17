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

/*
device_id,
SUM(bytes_to_srv) AS inbound_communication,     -- Equivalent to bytesIn
SUM(bytes_to_client) AS outbound_communication, -- Equivalent to bytesOut
COUNT(*) AS passive,                            -- Number of activity records
ROUND(100 * COUNT(*) / 3000, 2) AS percent_frequency -- Calculate activity % based on 3000 baseline
*/
type DevicesByCommFrequency struct {
	BaseAnalyticEntity
	StreamId  string `json:"-"`
	DeviceId  string `json:"deviceId"   bq:"device_id"`
	StartTime int64  `json:"timestamp"  bq:"start_time"`
	DataIn    int64  `json:"dataIn"     bq:"bytes_to_client"`
	DataOut   int64  `json:"dataOut"    bq:"bytes_to_srv"`

	Passive          int64 `json:"passive"    bq:"*"`
	PercentFrequency int   `json:"percent_frequency"`
}

func (f *DevicesByCommFrequency) KEY() string {
	return f.StreamId
}

func (f *DevicesByCommFrequency) TABLE() string {
	return fmt.Sprintf("flow-data-%s", f.KEY())
}

func NewDevicesByCommFrequency(shardKey string) entity.EntityFactory {
	return func() entity.Entity {
		shardKey := shardKey
		return &DevicesByCommFrequency{
			StreamId: shardKey,
		}
	}
}

func TestDevicesByCommFrequency(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}

	from, _ := ConvertToTimestamp("01-01-2025 00:00")
	to, _ := ConvertToTimestamp("10-01-2025 00:00")

	maxNumOfFlows := int64((to - from) / 30000)

	shardKey := "juuice-1"
	factory := NewDevicesByCommFrequency(shardKey)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	out := make([]*DevicesByCommFrequency, 0, 1000)
	cb := func(e entity.Entity) entity.Entity {

		r := e.(*DevicesByCommFrequency)
		r.PercentFrequency = int(100 * r.Passive / maxNumOfFlows)
		out = append(out, r)
		return nil
	}

	qa := bqdb.AdvancedQuery(factory)
	qa.Filter(F("start_time").Between(from, to)).
		Apply(cb).
		Sort("device_id-")

	_, err = qa.CountAll("*").
		Sum("bytes_to_srv").
		Sum("bytes_to_client").
		GroupBy("device_id", entity.TimePeriodCodes.UNDEFINED).
		Compute()

	if err != nil {
		t.Fatal(err)
	}

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	// Print the result
	fmt.Println(string(jsonData))

}
