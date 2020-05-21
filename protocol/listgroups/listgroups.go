package listgroups

import (
	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	_ struct{} `kafka:"min=v0,max=v0"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.ListGroups }

type Response struct {
	ErrorCode int16   `kafka:"min=v0,max=v0"`
	Groups    []Group `kafka:"min=v0,max=v0"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ListGroups }

type Group struct {
	GroupID      string `kafka:"min=v0,max=v0"`
	ProtocolType string `kafka:"min=v0,max=v0"`
}
