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

type PortDistribution struct {
	entity.BaseAnalyticEntity
	StreamId  string `json:"-"`
	StartTime int64  `json:"-"          bq:"start_time"`
	DstPort   int64  `json:"dstPort"    bq:"dst_port"`
	PortCount int64  `json:"portCount"  bq:"count"` //flag that triggers  " COUNT(*) "  output in result SQL
}

func (f *PortDistribution) KEY() string {
	return f.StreamId
}

func (f *PortDistribution) TABLE() string {
	return fmt.Sprintf("flow-data-%s", f.KEY())
}

func NewPortDistribution(shardKey string) entity.EntityFactory {
	return func() entity.Entity {
		shardKey := shardKey
		return &PortDistribution{
			StreamId: shardKey,
		}
	}
}

func TestPortDistribution(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
	shardKey := "etecnic-1"
	factory := NewPortDistribution(shardKey)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	qa := bqdb.AdvancedQuery(factory)

	qa.Filter(F("start_time").Between(1742083200000, 1742774400000)).
		Sort("count-").
		Limit(20)

	entities, err := qa.GroupBy("dst_port", entity.TimePeriodCodes.UNDEFINED).
		CountAll("count").
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
