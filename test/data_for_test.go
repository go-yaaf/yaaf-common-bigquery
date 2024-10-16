package test

import (
	"strconv"

	. "github.com/go-yaaf/yaaf-common/entity"
)

// FlowRecord represents a single flow record from the CSV data
// generated by Suricata-based parser
type FlowRecord struct {
	BaseEntity
	FlowId        int64  `json:"flow_id"`
	StreamId      string `json:"stream_id"`
	DeviceId      string `json:"device_id"`
	StartTime     int64  `json:"start_time"`
	EndTime       int64  `json:"end_time"`
	SrcIP         string `json:"src_ip"`
	SrcPort       int    `json:"src_port"`
	DstIP         string `json:"dst_ip"`
	DstPort       int    `json:"dst_port"`
	PcktToSrv     int    `json:"pckt_to_srv"`
	PcktToClient  int    `json:"pckt_to_client"`
	BytesToSrv    int    `json:"bytes_to_srv"`
	BytesToClient int    `json:"bytes_to_client"`
	Proto         string `json:"proto"`
	Pcap          string `json:"pcap"`
}

func (f *FlowRecord) TABLE() string { return "flow-data" }
func (f *FlowRecord) KEY() string   { return strconv.FormatInt(f.FlowId, 10) }

func NewFlowRecordEntity() Entity {
	return &FlowRecord{}
}
