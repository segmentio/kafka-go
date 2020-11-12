package electleaders

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	ElectionType    int8                    `kafka:"min=v1,max=v1"`
	TopicPartitions []RequestTopicPartition `kafka:"min=v0,max=v1"`
	TimeoutMs       int32                   `kafka:"min=v0,max=v1"`
}

type RequestTopicPartition struct {
	Topic       string
	PartitionID int32
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.ElectLeaders }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type Response struct {
	ThrottleTime           int32                           `kafka:"min=v0,max=v1"`
	ErrorCode              int16                           `kafka:"min=v1,max=v1"`
	ReplicaElectionResults []ResponseReplicaElectionResult `kafka:"min=v0,max=v1"`
}

type ResponseReplicaElectionResult struct {
	Topic            string                    `kafka:"min=v0,max=v1"`
	PartitionResults []ResponsePartitionResult `kafka:"min=v0,max=v1"`
}

type ResponsePartitionResult struct {
	PartitionID  int32  `kafka:"min=v0,max=v1"`
	ErrorCode    int16  `kafka:"min=v0,max=v1"`
	ErrorMessage string `kafka:"min=v0,max=v1,nullable"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.ElectLeaders }
