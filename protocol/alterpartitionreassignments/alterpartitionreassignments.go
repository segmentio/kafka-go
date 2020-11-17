package alterpartitionreassignments

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	TimeoutMs int32          `kafka:"min=v0,max=v0"`
	Topics    []RequestTopic `kafka:"min=v0,max=v0"`
}

type RequestTopic struct {
	Name       string              `kafka:"min=v0,max=v0,compact"`
	Partitions []RequestPartitions `kafka:"min=v0,max=v0"`
}

type RequestPartitions struct {
	PartitionIndex int32 `kafka:"min=v0,max=v0"`
	Replicas       int32 `kafka:"min=v0,max=v0"`
}

func (r *Request) ApiKey() protocol.ApiKey {
	return protocol.AlterPartitionReassignments
}

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

type Response struct {
}

func (r *Response) ApiKey() protocol.ApiKey {
	return protocol.AlterPartitionReassignments
}
