package incrementalalterconfigs

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	Resources    []RequestResource `kafka:"min=v0,max=v0"`
	ValidateOnly bool              `kafka:"min=v0,max=v0"`
}

type RequestResource struct {
	ResourceType int8            `kafka:"min=v0,max=v0"`
	ResourceName string          `kafka:"min=v0,max=v0"`
	Configs      []RequestConfig `kafka:"min=v0,max=v0"`
}

type RequestConfig struct {
	Name            string `kafka:"min=v0,max=v0"`
	ConfigOperation int8   `kafka:"min=v0,max=v0"`
	Value           string `kafka:"min=v0,max=v0,nullable"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.IncrementalAlterConfigs }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type Response struct {
	ThrottleTimeMs int32                   `kafka:"min=v0,max=v0"`
	Responses      []ResponseAlterResponse `kafka:"min=v0,max=v0"`
}

type ResponseAlterResponse struct {
	ErrorCode    int16  `kafka:"min=v0,max=v0"`
	ErrorMessage string `kafka:"min=v0,max=v0,nullable"`
	ResourceType int8   `kafka:"min=v0,max=v0"`
	ResourceName string `kafka:"min=v0,max=v0"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.IncrementalAlterConfigs }
