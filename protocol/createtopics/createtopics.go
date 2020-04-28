package createtopics

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	Topics       []RequestTopic `kafka:"min=v0,max=v5"`
	TimeoutMs    int32          `kafka:"min=v0,max=v5"`
	ValidateOnly bool           `kafka:"min=v1,max=v5"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.CreateTopics }

type RequestTopic struct {
	Name              string              `kafka:"min=v0,max=v4|min=v5,max=v5,compact"`
	NumPartitions     int32               `kafka:"min=v0,max=v5"`
	ReplicationFactor int16               `kafka:"min=v0,max=v5"`
	Assignments       []RequestAssignment `kafka:"min=v0,max=v5"`
	Configs           []RequestConfig     `kafka:"min=v0,max=v5"`
}

type RequestAssignment struct {
	PartitionIndex int32   `kafka:"min=v0,max=v5"`
	BrokerIDs      []int32 `kafka:"min=v0,max=v5"`
}

type RequestConfig struct {
	Name  string `kafka:"min=v0,max=v4|min=v5,max=v5,compact"`
	Value string `kafka:"min=v0,max=v4,nullable|min=v5,max=v5,compact,nullable"`
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v2,max=v5"`
	Topics         []ResponseTopic `kafka:"min=v0,max=v5"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.CreateTopics }

type ResponseTopic struct {
	Name              string           `kafka:"min=v0,max=v4|min=v5,max=v5,compact"`
	ErrorCode         int16            `kafka:"min=v0,max=v5"`
	ErrorMessage      string           `kafka:"min=v1,max=v4,nullable|min=v5,max=v5,compact,nullable"`
	NumPartitions     int32            `kafka:"min=v5,max=v5"`
	ReplicationFactor int16            `kafka:"min=v5,max=v5"`
	Configs           []ResponseConfig `kafka:"min=v5,max=v5"`
}

type ResponseConfig struct {
	Name         string `kafka:"min=v5,max=v5,compact"`
	Value        string `kafka:"min=v5,max=v5,compact,nullable"`
	ReadOnly     bool   `kafka:"min=v5,max=v5"`
	ConfigSource int8   `kafka:"min=v5,max=v5"`
	IsSensitive  bool   `kafka:"min=v5,max=v5"`
}
