package saslauthenticate

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	AuthBytes []byte `kafka:"min=v0,max=v1|min=v2,max=v2,compact"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.SaslAuthenticate }

type Response struct {
	ErrorCode         int16  `kafka:"min=v0,max=v2"`
	ErrorMessage      string `kafka:"min=v0,max=v1,nullable|min=v2,max=v2,compact,nullable"`
	AuthBytes         []byte `kafka:"min=v0,max=v1|min=v2,max=v2,compact"`
	SessionLifetimeMs int64  `kafka:"min=v1,max=v2"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.SaslAuthenticate }
