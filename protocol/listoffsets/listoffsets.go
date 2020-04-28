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
}

type Response struct {
	Topics []ResponseTopic `kafka:"min=v1,max=v5"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ListOffsets }

type ResponseTopic struct {
	Topic      string              `kafka:"min=v1,max=v5"`
	Partitions []ResponsePartition `kafka:"min=v1,max=v5"`
}

type ResponsePartition struct {
	ThrottleTimeMs int32 `kafka:"min=v2,max=v5"`
	Partition      int32 `kafka:"min=v1,max=v5"`
	ErrorCode      int16 `kafka:"min=v1,max=v5"`
	Timestamp      int64 `kafka:"min=v1,max=v5"`
	Offset         int64 `kafka:"min=v1,max=v5"`
	LeaderEpoch    int32 `kafka:"min=v4,max=v5"`
}
