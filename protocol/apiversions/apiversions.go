package apiversions

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	_                     struct{} `kafka:"min=v0,max=v3"`
	ClientSoftwareName    string   `kafka:"min=v3,max=v3,compact"`
	ClientSoftwareVersion string   `kafka:"min=v3,max=v3,compact"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.ApiVersions }

type Response struct {
	ErrorCode      int16            `kafka:"min=v0,max=v3"`
	ApiKeys        []ApiKeyResponse `kafka:"min=v0,max=v3"`
	ThrottleTimeMs int32            `kafka:"min=v1,max=v3"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ApiVersions }

type ApiKeyResponse struct {
	ApiKey     int16 `kafka:"min=v0,max=v3"`
	MinVersion int16 `kafka:"min=v0,max=v3"`
	MaxVersion int16 `kafka:"min=v0,max=v3"`
}
