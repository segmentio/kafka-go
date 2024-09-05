package describelogdirs

import (
	"fmt"

	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	Topics   []RequestTopic `kafka:"min=v0,max=v3"`
	BrokerID int32          `kafka:"-"`
}

type RequestTopic struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	Topic      string  `kafka:"min=v0,max=v1|min=v2,max=v3,compact"`
	Partitions []int32 `kafka:"min=v0,max=v3"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DescribeLogDirs }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	// Check that if the broker exists, will produce the wrong result
	// if that's not the case.
	broker, ok := cluster.Brokers[r.BrokerID]
	if !ok {
		return broker, fmt.Errorf("could not find broker: %d", r.BrokerID)
	}
	return broker, nil
}

type Response struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	ThrottleTimeMs int32             `kafka:"min=v0,max=v3"`
	ErrorCode      int16             `kafka:"min=v3,max=v3"`
	Results        []ResponseLogDirs `kafka:"min=v0,max=v3"`
}

type ResponseLogDirs struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	ErrorCode int16           `kafka:"min=v0,max=v3"`
	LogDir    string          `kafka:"min=v0,max=v1|min=v2,max=v3,compact"`
	Topics    []ResponseTopic `kafka:"min=v0,max=v3"`
}

type ResponseTopic struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	Name       string              `kafka:"min=v0,max=v1|min=v2,max=v3,compact"`
	Partitions []ResponsePartition `kafka:"min=v0,max=v3"`
}

type ResponsePartition struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v3,tag"`

	PartitionIndex int32 `kafka:"min=v0,max=v3"`
	PartitionSize  int64 `kafka:"min=v0,max=v3"`
	OffsetLag      int64 `kafka:"min=v0,max=v3"`
	IsFutureKey    bool  `kafka:"min=v0,max=v3"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DescribeLogDirs }
