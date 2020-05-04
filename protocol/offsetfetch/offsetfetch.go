package offsetfetch

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	GroupID       string         `kafka:"min=v0,max=v5|min=v6,max=v7,compact""`
	Topics        []RequestTopic `kafka:"min=v0,max=v7"`
	RequireStable bool           `kafka:"min=v7,max=v7"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.OffsetFetch }

func (r *Request) Group() string { return r.GroupID }

type RequestTopic struct {
	Name             string  `kafka:"min=v0,max=v5|min=v6,max=v7,compact"`
	PartitionIndexes []int32 `kafka:"min=v0,max=v7"`
}

var (
	_ protocol.GroupMessage = (*Request)(nil)
)

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v3,max=v7"`
	Topics         []ResponseTopic `kafka:"min=v0,max=v7"`
	ErrorCode      int16           `kafka:"min=v2,max=v7"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.OffsetFetch }

type ResponseTopic struct {
	Name       string              `kafka:"min=v0,max=v5|min=v6,max=v7,compact"`
	Partitions []ResponsePartition `kafka:"min=v0,max=v7"`
}

type ResponsePartition struct {
	PartitionIndex      int32  `kafka:"min=v0,max=v7"`
	CommittedOffset     int64  `kafka:"min=v0,max=v7"`
	ComittedLeaderEpoch int32  `kafka:"min=v5,max=v7"`
	Metadata            string `kafka:"min=v0,max=v5,nullable|min=v6,max=v7,compact,nullable"`
	ErrorCode           int16  `kafka:"min=v0,max=v7"`
}
