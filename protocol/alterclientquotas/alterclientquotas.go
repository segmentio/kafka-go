package alterclientquotas

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v2,tag"`

	Entries      []Entry `kafka:"min=v0,max=v1"`
	ValidateOnly bool    `kafka:"min=v0,max=v1"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.AlterClientQuotas }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type Entry struct {
	Entities []Entity `kafka:"min=v0,max=v1"`
	Ops      []Ops    `kafka:"min=v0,max=v1"`
}

type EntityType string

const (
	ClientID EntityType = "client-id"
	User     EntityType = "user"
)

type Entity struct {
	EntityType EntityType `kafka:"min=v0,max=v1"`
	EntityName string     `kafka:"min=v0,max=v1,nullable"`
}

type Ops struct {
	Key    string  `kafka:"min=v0,max=v1"`
	Value  float64 `kafka:"min=v0,max=v1"`
	Remove bool    `kafka:"min=v0,max=v1"`
}

type Response struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v2,tag"`

	ThrottleTimeMs int32            `kafka:"min=v0,max=v1"`
	Results        []ResponseQuotas `kafka:"min=v0,max=v1"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.AlterClientQuotas }

type ResponseQuotas struct {
	ErrorCode    int16    `kafka:"min=v0,max=v1"`
	ErrorMessage string   `kafka:"min=v0,max=v1,nullable"`
	Entities     []Entity `kafka:"min=v0,max=v1"`
}

var _ protocol.BrokerMessage = (*Request)(nil)
