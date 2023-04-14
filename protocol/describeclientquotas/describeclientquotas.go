package describeclientquotas

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v2,tag"`

	Components []Component `kafka:"min=v0,max=v1"`
	Strict     boolean     `kafka:"min=v0,max=v1"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DescribeClientQuotas }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type Component struct {
	EntityType string `kafka:"min=v0,max=v1"`
	MatchType  int8   `kafka:"min=v0,max=v1"`
	Match      string `kafka:"min=v0,max=v1,nullable"`
}

type Response struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v2,tag"`

	ThrottleTimeMs int32            `kafka:"min=v0,max=v1"`
	ErrorCode      int16            `kafka:"min=v0,max=v1"`
	ErrorMessage   string           `kafka:"min=v0,max=v1,nullable"`
	Entries        []ResponseQuotas `kafka:"min=v0,max=v1"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DescribeClientQuotas }

type Value struct {
	Key   string  `kafka:"min=v0,max=v1"`
	Value float64 `kafka:"min=v0,max=v1"`
}

type ResponseQuotas struct {
	Entities []Entity `kafka:"min=v0,max=v1"`
	Values   []Value  `kafka:"min=v0,max=v1"`
}

var _ protocol.BrokerMessage = (*Request)(nil)
