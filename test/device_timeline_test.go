package test

import (
	"fmt"
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
	. "github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
)

type DeviceConsumption struct {
	entity.BaseEntity
	StreamId  string `json:"-"`
	Timestamp int64  `json:"timestamp" bq:"start_time" `
	Protocol  string `json:"-"         bq:"protocol"`
	DstPort   int    `json:"-"         bq:"dst_port"`
	DataOut   int64  `json:"dataOut"   bq:"bytes_to_srv"  `
	DataIn    int64  `json:"dataIn"    bq:"bytes_to_client"  `
	Total     int64  `json:"total"     `
}

func (f *DeviceConsumption) KEY() string {
	return f.StreamId
}

func (f *DeviceConsumption) TABLE() string {
	return fmt.Sprintf("flow-data-%s", f.KEY())
}

func NewDeviceConsumption(shardKey string) entity.EntityFactory {
	return func() entity.Entity {
		shardKey := shardKey
		return &DeviceConsumption{
			StreamId: shardKey,
		}
	}
}

func TestDeviceTimeLine(t *testing.T) {

	shardKey := "etecnic-1"
	factory := NewDeviceConsumption(shardKey)

	bqdb, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	cb := func(e entity.Entity) entity.Entity {
		dt := e.(*DeviceConsumption)
		dt.Total = dt.DataIn + dt.DataOut
		return dt
	}

	qa := bqdb.AdvancedQuery(factory)
	qa.Filter(F("protocol").In([]string{"TCP", "UDP"})).
		Sort("timestamp-").
		Apply(cb)

	entities, err := qa.Sum("bytes_to_srv").
		Sum("bytes_to_client").
		GroupBy("start_time", entity.TimePeriodCodes.DAY).
		Compute()

	if err != nil {
		t.Fatal(err)
	}
	logger.Debug("count: %d", len(entities))
}
