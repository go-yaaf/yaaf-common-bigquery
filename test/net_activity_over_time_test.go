package test

import (
	"fmt"
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
)

type NewtworkActivityOverTime struct {
	entity.BaseEntity
	StreamId     string `json:"-"`
	TimePoint    int64  `json:"timestamp" bq:"start_time"`
	NumOfDevices int64  `json:"value"     bq:"device_id"`
}

func (f *NewtworkActivityOverTime) KEY() string {
	return f.StreamId
}

func (f *NewtworkActivityOverTime) TABLE() string {
	return fmt.Sprintf("flow-data-%s", f.KEY())
}

func NewNetworkActivityOverTime(shardKey string) entity.EntityFactory {
	return func() entity.Entity {
		shardKey := shardKey
		return &NewtworkActivityOverTime{
			StreamId: shardKey,
		}
	}
}
func TestNetworkActivityOverTime(t *testing.T) {
	shardKey := "etecnic-1"
	factory := NewNetworkActivityOverTime(shardKey)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	qa := bqdb.AdvancedQuery(factory)
	qa.Filter(F("start_time").Between(1735776000000, 1736467200000)).
		Sort("timestamp-")

	entities, err := qa.GroupBy("start_time", entity.TimePeriodCodes.HOUR).
		CountUnique("device_id").
		Compute()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", len(entities))

}
