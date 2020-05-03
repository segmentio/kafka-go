package produce

import (
	"fmt"

	"github.com/segmentio/kafka-go/protocol"
)

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	TransactionalID string         `kafka:"min=v3,max=v8,nullable"`
	Acks            int16          `kafka:"min=v0,max=v8"`
	Timeout         int32          `kafka:"min=v0,max=v8"`
	Topics          []RequestTopic `kafka:"min=v0,max=v8"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.Produce }

func (r *Request) Close() error {
	for i := range r.Topics {
		t := &r.Topics[i]
		for j := range t.Partitions {
			p := &t.Partitions[j]
			p.RecordSet.Close()
		}
	}
	return nil
}

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	var broker protocol.Broker

	for i := range r.Topics {
		t := &r.Topics[i]

		topic, ok := cluster.Topics[t.Topic]
		if !ok {
			return protocol.Broker{}, fmt.Errorf("produce request topic not found in cluster metadata: topic=%q", t.Topic)
		}

		for j := range t.Partitions {
			p := &t.Partitions[j]

			partition, ok := topic.Partitions[int(p.Partition)]
			if !ok {
				return protocol.Broker{}, fmt.Errorf("produce request topic partition not found in cluster metadata: topic=%q partition=%d",
					t.Topic, p.Partition)
			}

			if b, ok := cluster.Brokers[partition.Leader]; !ok {
				return protocol.Broker{}, fmt.Errorf("produce request partition leader not found in cluster metadata: topic=%q partition=%d broker=%d",
					t.Topic, p.Partition, partition.Leader)
			} else if broker == (protocol.Broker{}) {
				broker = b
			} else if b.ID != broker.ID {
				return protocol.Broker{}, fmt.Errorf("produce request contained partitions with mismatching leaders: topic=%q partition=%d broker=%d",
					t.Topic, p.Partition, b.ID)
			}
		}
	}

	return broker, nil
}

func (r *Request) Prepare(apiVersion int16) {
	// Determine which version of the message should be used, based on which
	// version of the Produce API is supported by the server.
	//
	// In version 0.11, kafka gives this error:
	//
	//   org.apache.kafka.common.record.InvalidRecordException
	//   Produce requests with version 3 are only allowed to contain record batches with magic version.
	//
	// In version 2.x, kafka refuses the message claiming that the CRC32
	// checksum is invalid.
	var recordVersion int8

	if apiVersion < 3 {
		recordVersion = 1
	} else {
		recordVersion = 2
	}

	for i := range r.Topics {
		t := &r.Topics[i]

		for j := range t.Partitions {
			p := &t.Partitions[j]

			// Allow the program to overload the version if really needed.
			if p.RecordSet.Version == 0 {
				p.RecordSet.Version = recordVersion
			}
		}
	}
}

type RequestTopic struct {
	Topic      string             `kafka:"min=v0,max=v8"`
	Partitions []RequestPartition `kafka:"min=v0,max=v8"`
}

type RequestPartition struct {
	Partition int32              `kafka:"min=v0,max=v8"`
	RecordSet protocol.RecordSet `kafka:"min=v0,max=v8"`
}

type Response struct {
	Topics         []ResponseTopic `kafka:"min=v0,max=v8"`
	ThrottleTimeMs int32           `kafka:"min=v1,max=v8"`
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.Produce }

type ResponseTopic struct {
	Topic      string              `kafka:"min=v0,max=v8"`
	Partitions []ResponsePartition `kafka:"min=v0,max=v8"`
}

type ResponsePartition struct {
	Partition      int32           `kafka:"min=v0,max=v8"`
	ErrorCode      int16           `kafka:"min=v0,max=v8"`
	BaseOffset     int64           `kafka:"min=v0,max=v8"`
	LogAppendTime  int64           `kafka:"min=v2,max=v8"`
	LogStartOffset int64           `kafka:"min=v5,max=v8"`
	RecordErrors   []ResponseError `kafka:"min=v8,max=v8"`
	ErrorMessage   string          `kafka:"min=v8,max=v8,nullable"`
}

type ResponseError struct {
	BatchIndex             int32  `kafka:"min=v8,max=v8"`
	BatchIndexErrorMessage string `kafka:"min=v8,max=v8,nullable"`
}

var (
	_ protocol.BrokerMessage   = (*Request)(nil)
	_ protocol.PreparedMessage = (*Request)(nil)
)
