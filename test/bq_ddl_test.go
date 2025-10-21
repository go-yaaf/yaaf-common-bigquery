package test

import (
	"os"
	"testing"

	bigquerydb "github.com/go-yaaf/yaaf-common-bigquery/bqdb"
)

var flowTableSql = `
CREATE TABLE IF NOT EXISTS %s (
  flow_id         INT64      NOT NULL,
  device_id       STRING     NOT NULL,
  start_time      TIMESTAMP  NOT NULL,     
  end_time        TIMESTAMP  NOT NULL,     
  src_ip          STRING     NOT NULL,
  src_port        INT64      NOT NULL,
  dst_ip          STRING     NOT NULL,
  dst_port        INT64      NOT NULL,
  pckt_to_srv     INT64      NOT NULL,
  pckt_to_client  INT64      NOT NULL,
  bytes_to_srv    INT64      NOT NULL,
  bytes_to_client INT64      NOT NULL,
  protocol        STRING     NOT NULL, 
  pcap            STRING     NOT NULL,
  alerted         BOOL       NOT NULL 
)
PARTITION BY DATE(start_time)
CLUSTER BY device_id, dst_ip, dst_port, protocol
OPTIONS (
      require_partition_filter = TRUE,
      partition_expiration_days = 30
    );
`

var deviceComsumptionMvSql = `
CREATE MATERIALIZED VIEW IF NOT EXISTS
  %s
AS
SELECT
  TIMESTAMP_TRUNC(start_time, DAY) AS start_time,
  device_id,
  dst_ip,
  dst_port,
  protocol,
  SUM(bytes_to_srv)    AS dataOut,
  SUM(bytes_to_client) AS dataIn
FROM %s
GROUP BY start_time, device_id, dst_ip, dst_port, protocol;
`

func TestBqddlsql(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}

	db, err := bigquerydb.NewBqDatabase("bq://shieldiot-staging:pulseiot")

	if err != nil {
		t.Fatalf("NewBqDatabase failed: %v", err)
	}

	var ddl = make(map[string][]string, 0)

	ddl["shardKey"] = []string{"test"}
	ddl["name"] = []string{"flow-data", "mv-device-consumption"}
	ddl["sql"] = []string{flowTableSql, deviceComsumptionMvSql}

	if err := db.ExecuteDDL(ddl); err != nil {
		t.Fatal("db.ExecuteDDL failed:  " + err.Error())
	}

}
