package listoffsets

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	ReplicaID      int32          `kafka:"min=v1,max=v5"`
	IsolationLevel int8           `kafka:"min=v2,max=v5"`
	Topics         []RequestTopic `kafka:"min=v1,max=v5"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.ListOffsets }

type RequestTopic struct {
	Topic      string             `kafka:"min=v1,max=v5"`
	Partitions []RequestPartition `kafka:"min=v1,max=v5"`
}

type RequestPartition struct {
	Partition          int32 `kafka:"min=v1,max=v5"`
	CurrentLeaderEpoch int32 `kafka:"min=v4,max=v5"`
	Timestamp          int64 `kafka:"min=v1,max=v5"`
	// v0 of the API predates kafka 0.10, and doesn't make much sense to
	// use so we chose not to support it. It had this extra field to limit
	// the number of offsets returned, which has been removed in v1.
	//
	// MaxNumOffsets int32 `kafka:"min=v0,max=v0"`
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v2,max=v5"`
	Topics         []ResponseTopic `kafka:"min=v1,max=v5"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ListOffsets }

type ResponseTopic struct {
	Topic      string              `kafka:"min=v1,max=v5"`
	Partitions []ResponsePartition `kafka:"min=v1,max=v5"`
}

type ResponsePartition struct {
	Partition   int32 `kafka:"min=v1,max=v5"`
	ErrorCode   int16 `kafka:"min=v1,max=v5"`
	Timestamp   int64 `kafka:"min=v1,max=v5"`
	Offset      int64 `kafka:"min=v1,max=v5"`
	LeaderEpoch int32 `kafka:"min=v4,max=v5"`
}
