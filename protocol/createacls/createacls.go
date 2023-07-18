package createacls

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v2,tag"`

	Creations []RequestACLs `kafka:"min=v0,max=v2"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.CreateAcls }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type RequestACLs struct {
	ResourceType        int8   `kafka:"min=v0,max=v2"`
	ResourceName        string `kafka:"min=v0,max=v1|min=v2,max=v2,compact"`
	ResourcePatternType int8   `kafka:"min=v1,max=v2"`
	Principal           string `kafka:"min=v0,max=v1|min=v2,max=v2,compact"`
	Host                string `kafka:"min=v0,max=v2|min=v2,max=v2,compact"`
	Operation           int8   `kafka:"min=v0,max=v2"`
	PermissionType      int8   `kafka:"min=v0,max=v2"`
}

type Response struct {
	// We need at least one tagged field to indicate that v2+ uses "flexible"
	// messages.
	_ struct{} `kafka:"min=v2,max=v2,tag"`

	ThrottleTimeMs int32          `kafka:"min=v0,max=v2"`
	Results        []ResponseACLs `kafka:"min=v0,max=v2"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.CreateAcls }

type ResponseACLs struct {
	ErrorCode    int16  `kafka:"min=v0,max=v2"`
	ErrorMessage string `kafka:"min=v0,max=v1,nullable|min=v2,max=v2,nullable,compact"`
}

var _ protocol.BrokerMessage = (*Request)(nil)
