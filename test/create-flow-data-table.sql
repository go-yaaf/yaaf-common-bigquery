CREATE TABLE `shieldiot-staging.pulseiot.flow-data` (
  flow_id INT64 NOT NULL,
  device_id STRING NOT NULL,
  start_time INT64 NOT NULL,  
  end_time INT64 NOT NULL, 
  src_ip STRING NOT NULL,
  src_port INT64 NOT NULL,
  dst_ip STRING NOT NULL,
  dst_port INT64 NOT NULL,
  pckt_to_srv INT64 NOT NULL,
  pckt_to_client INT64 NOT NULL,
  bytes_to_srv INT64 NOT NULL,
  bytes_to_client INT64 NOT NULL,
  protocol STRING NOT NULL, 
  pcap STRING NOT NULL,
  alerted BOOL NOT NULL 
)
CLUSTER BY dst_ip,src_ip;          
