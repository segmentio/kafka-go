package txnoffsetcommit

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	TransactionalID string         `kafka:"min=v0,max=v2|min=v3,max=v5,compact"`
	GroupID         string         `kafka:"min=v0,max=v2|min=v3,max=v5,compact"`
	ProducerID      int64          `kafka:"min=v0,max=v5"`
	ProducerEpoch   int16          `kafka:"min=v0,max=v5"`
	GenerationID    int32          `kafka:"min=v3,max=v5"`
	MemberID        string         `kafka:"min=v3,max=v5,compact"`
	GroupInstanceID string         `kafka:"min=v3,max=v5,compact,nullable"`
	Topics          []RequestTopic `kafka:"min=v0,max=v5"`
}

type RequestTopic struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	Name       string             `kafka:"min=v0,max=v2|min=v3,max=v5,compact"`
	Partitions []RequestPartition `kafka:"min=v0,max=v5"`
}

type RequestPartition struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	Partition            int32  `kafka:"min=v0,max=v5"`
	CommittedOffset      int64  `kafka:"min=v0,max=v5"`
	CommittedLeaderEpoch int32  `kafka:"min=v2,max=v5"`
	CommittedMetadata    string `kafka:"min=v0,max=v2|min=v3,max=v5,nullable,compact"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.TxnOffsetCommit }

func (r *Request) Group() string { return r.GroupID }

var _ protocol.GroupMessage = (*Request)(nil)

type Response struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	ThrottleTimeMs int32           `kafka:"min=v0,max=v5"`
	Topics         []ResponseTopic `kafka:"min=v0,max=v5"`
}

type ResponseTopic struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	Name       string              `kafka:"min=v0,max=v2|min=v3,max=v5,compact"`
	Partitions []ResponsePartition `kafka:"min=v0,max=v5"`
}

type ResponsePartition struct {
	// We need at least one tagged field to indicate that this is a "flexible" message
	// type.
	_ struct{} `kafka:"min=v3,max=v5,tag"`

	Partition int32 `kafka:"min=v0,max=v5"`
	ErrorCode int16 `kafka:"min=v0,max=v5"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.TxnOffsetCommit }
