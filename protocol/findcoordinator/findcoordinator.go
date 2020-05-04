package findcoordinator

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	Key     string `kafka:"min=v0,max=v2|min=v3,max=v3,compact"`
	KeyType int8   `kafka:"min=v1,max=v3"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.FindCoordinator }

type Response struct {
	ThrottleTimeMs int32  `kafka:"min=v1,max=v3"`
	ErrorCode      int16  `kafka:"min=v0,max=v3"`
	ErrorMessage   string `kafka:"min=v1,max=v2,nullable|min=v3,max=v3,compact,nullable"`
	NodeID         int32  `kafka:"min=v0,max=v3"`
	Host           string `kafka:"min=v0,max=v2|min=v3,max=v3,compact"`
	Port           int32  `kafka:"min=v0,max=v3"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.FindCoordinator }
