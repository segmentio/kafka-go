package fetch

import (
	"fmt"

	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	ReplicaID       int32                   `kafka:"min=v0,max=v11"`
	MaxWaitTime     int32                   `kafka:"min=v0,max=v11"`
	MinBytes        int32                   `kafka:"min=v0,max=v11"`
	MaxBytes        int32                   `kafka:"min=v3,max=v11"`
	IsolationLevel  int8                    `kafka:"min=v4,max=v11"`
	SessionID       int32                   `kafka:"min=v7,max=v11"`
	SessionEpoch    int32                   `kafka:"min=v7,max=v11"`
	Topics          []RequestTopic          `kafka:"min=v0,max=v11"`
	ForgottenTopics []RequestForgottenTopic `kafka:"min=v7,max=v11"`
	RackID          string                  `kafka:"min=v11,max=v11"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.Fetch }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	var broker protocol.Broker

	for i := range r.Topics {
		t := &r.Topics[i]

		topic, ok := cluster.Topics[t.Topic]
		if !ok {
			return protocol.Broker{}, fmt.Errorf("fetch request topic not found in cluster metadata: topic=%q", t.Topic)
		}

		for j := range t.Partitions {
			p := &t.Partitions[j]

			partition, ok := topic.Partitions[int(p.Partition)]
			if !ok {
				return protocol.Broker{}, fmt.Errorf("fetch request topic partition not found in cluster metadata: topic=%q partition=%d",
					t.Topic, p.Partition)
			}

			if b, ok := cluster.Brokers[partition.Leader]; !ok {
				return protocol.Broker{}, fmt.Errorf("fetch request partition leader not found in cluster metadata: topic=%q partition=%d broker=%d",
					t.Topic, p.Partition, partition.Leader)
			} else if broker == (protocol.Broker{}) {
				broker = b
			} else if b.ID != broker.ID {
				return protocol.Broker{}, fmt.Errorf("fetch request contained partitions with mismatching leaders: topic=%q partition=%d broker=%d",
					t.Topic, p.Partition, b.ID)
			}
		}
	}

	return broker, nil
}

type RequestTopic struct {
	Topic      string             `kafka:"min=v0,max=v11"`
	Partitions []RequestPartition `kafka:"min=v0,max=v11"`
}

type RequestPartition struct {
	Partition          int32 `kafka:"min=v0,max=v11"`
	CurrentLeaderEpoch int32 `kafka:"min=v9,max=v11"`
	FetchOffset        int64 `kafka:"min=v0,max=v11"`
	LogStartOffset     int64 `kafka:"min=v5,max=v11"`
	PartitionMaxBytes  int32 `kafka:"min=v0,max=v11"`
}

type RequestForgottenTopic struct {
	Topic      string  `kafka:"min=v7,max=v11"`
	Partitions []int32 `kafka:"min=v7,max=v11"`
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v1,max=v11"`
	ErrorCode      int16           `kafka:"min=v7,max=v11"`
	SessionID      int32           `kafka:"min=v7,max=v11"`
	Topics         []ResponseTopic `kafka:"min=v0,max=v11"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.Fetch }

func (r *Response) Close() error {
	for i := range r.Topics {
		t := &r.Topics[i]

		for j := range t.Partitions {
			p := &t.Partitions[j]
			protocol.CloseRecordBatch(p.RecordSet.Records)
		}
	}

	return nil
}

func (r *Response) Reset() {
	for i := range r.Topics {
		t := &r.Topics[i]

		for j := range t.Partitions {
			p := &t.Partitions[j]
			protocol.ResetRecordBatch(p.RecordSet.Records)
		}
	}
}

type ResponseTopic struct {
	Topic      string              `kafka:"min=v0,max=v11"`
	Partitions []ResponsePartition `kafka:"min=v0,max=v11"`
}

type ResponsePartition struct {
	Partition            int32                 `kafka:"min=v0,max=v11"`
	ErrorCode            int16                 `kafka:"min=v0,max=v11"`
	HighWatermark        int64                 `kafka:"min=v0,max=v11"`
	LastStableOffset     int64                 `kafka:"min=v4,max=v11"`
	LogStartOffset       int64                 `kafka:"min=v5,max=v11"`
	AbortedTransactions  []ResponseTransaction `kafka:"min=v4,max=v11"`
	PreferredReadReplica int32                 `kafka:"min=v11,max=v11"`
	RecordSet            protocol.RecordSet    `kafka:"min=v0,max=v11"`
}

type ResponseTransaction struct {
	ProducerID  int64 `kafka:"min=v4,max=v11"`
	FirstOffset int64 `kafka:"min=v4,max=v11"`
}

var (
	_ protocol.BrokerMessage = (*Request)(nil)
)
