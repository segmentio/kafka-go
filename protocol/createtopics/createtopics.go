package createtopics

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	Topics       []RequestTopic `kafka:"min=v0,max=v4"`
	TimeoutMs    int32          `kafka:"min=v0,max=v4"`
	ValidateOnly bool           `kafka:"min=v1,max=v4"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.CreateTopics }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type RequestTopic struct {
	Name              string              `kafka:"min=v0,max=v4"`
	NumPartitions     int32               `kafka:"min=v0,max=v4"`
	ReplicationFactor int16               `kafka:"min=v0,max=v4"`
	Assignments       []RequestAssignment `kafka:"min=v0,max=v4"`
	Configs           []RequestConfig     `kafka:"min=v0,max=v4"`
}

type RequestAssignment struct {
	PartitionIndex int32   `kafka:"min=v0,max=v4"`
	BrokerIDs      []int32 `kafka:"min=v0,max=v4"`
}

type RequestConfig struct {
	Name  string `kafka:"min=v0,max=v4"`
	Value string `kafka:"min=v0,max=v4,nullable"`
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v2,max=v4"`
	Topics         []ResponseTopic `kafka:"min=v0,max=v4"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.CreateTopics }

type ResponseTopic struct {
	Name         string `kafka:"min=v0,max=v4"`
	ErrorCode    int16  `kafka:"min=v0,max=v4"`
	ErrorMessage string `kafka:"min=v1,max=v4,nullable"`
}

var (
	_ protocol.BrokerMessage = (*Request)(nil)
)
