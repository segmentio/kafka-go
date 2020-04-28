package deletetopics

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	TopicNames []string `kafka:"min=v0,max=v3|min=v4,max=v4,compact"`
	TimeoutMs  int32    `kafka:"min=v0,max=v4"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DeleteTopics }

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v1,max=v4"`
	Responses      []ResponseTopic `kafka:"min=v0,max=v4"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DeleteTopics }

type ResponseTopic struct {
	Name      string `kafka:"min=v0,max=v3|min=v4,max=v4,compact"`
	ErrorCode int16  `kafka:"min=v0,max=v4"`
}
