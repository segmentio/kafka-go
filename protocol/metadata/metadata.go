package metadata

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	TopicNames                          []string `kafka:"min=v0,max=v8,nullable|min=v9,max=v9,compact,nullable"`
	AllowAutoTopicCreation              bool     `kafka:"min=v4,max=v9"`
	IncludeCustomerAuthorizedOperations bool     `kafka:"min=v8,max=v9"`
	IncludeTopicAuthorizedOperations    bool     `kafka:"min=v8,max=v9"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.Metadata }

type Response struct {
	ThrottleTimeMs              int32            `kafka:"min=v3,max=v9"`
	Brokers                     []ResponseBroker `kafka:"min=v0,max=v9"`
	ClusterID                   string           `kafka:"min=v2,max=v9,nullable"`
	ControllerID                int32            `kafka:"min=v1,max=v9"`
	Topics                      []ResponseTopic  `kafka:"min=v0,max=v9"`
	ClusterAuthorizedOperations int32            `kafka:"min=v8,max=v9"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.Metadata }

type ResponseBroker struct {
	NodeID int32  `kafka:"min=v0,max=v9"`
	Host   string `kafka:"min=v0,max=v8|min=v9,max=v9,compact"`
	Port   int32  `kafka:"min=v0,max=v9"`
	Rack   string `kafka:"min=v1,max=v9,nullable|min=v9,max=v9,compact,nullable"`
}

type ResponseTopic struct {
	ErrorCode                 int16               `kafka:"min=v0,max=v9"`
	Name                      string              `kafka:"min=v0,max=v9,nullable|min=v9,max=v9,compact,nullable"`
	IsInternal                bool                `kafka:"min=v1,max=v9"`
	Partitions                []ResponsePartition `kafka:"min=v0,max=v9"`
	TopicAuthorizedOperations int32               `kafka:"min=v8,max=v9"`
}

type ResponsePartition struct {
	ErrorCode       int16   `kafka:"min=v0,max=v9"`
	PartitionIndex  int32   `kafka:"min=v0,max=v9"`
	LeaderID        int32   `kafka:"min=v0,max=v9"`
	LeaderEpoch     int32   `kafka:"min=v7,max=v9"`
	ReplicaNodes    []int32 `kafka:"min=v0,max=v9"`
	IsrNodes        []int32 `kafka:"min=v0,max=v9"`
	OfflineReplicas []int32 `kafka:"min=v5,max=v9"`
}
