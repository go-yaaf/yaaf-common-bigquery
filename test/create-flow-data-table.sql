CREATE TABLE `your_project_id.your_dataset_id.flow-data` (
  flow_id INT64 NOT NULL,
  device_id STRING NOT NULL,
  start_time TIMESTAMP NOT NULL, -- Changed to TIMESTAMP for proper partitioning
  end_time TIMESTAMP NOT NULL,
  src_ip STRING NOT NULL,
  src_port INT64 NOT NULL,
  dst_ip STRING NOT NULL,
  dst_port INT64 NOT NULL,
  pckt_to_srv INT64 NOT NULL,
  pckt_to_client INT64 NOT NULL,
  bytes_to_srv INT64 NOT NULL,
  bytes_to_client INT64 NOT NULL,
  protocol STRING NOT NULL, -- Changed from "proto" to "protocol"
  pcap STRING NOT NULL,
  alerted BOOL NOT NULL,
  stream_id STRING NOT NULL
)
PARTITION BY DATE(start_time) -- Partitioned by DATE derived from start_time
CLUSTER BY stream_id;         -- Clustered by stream_id for optimization

