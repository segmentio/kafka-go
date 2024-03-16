package kafka

import (
	"context"
	"fmt"
	"net"

	"github.com/segmentio/kafka-go/protocol/describelogdirs"
)

// DescribeLogDirsRequest is a request to the DescribeLogDirs API.
type DescribeLogDirsRequest struct {
	// Addr is the address of the kafka broker to send the request to.
	Addr net.Addr

	// BrokerIDs is a slice of brokers to get log directories for.
	// if not set, every broker will be set automatically.
	BrokerIDs []int
}

// DescribeLogDirsResponse is a response from the DescribeLogDirs API.
type DescribeLogDirsResponse struct {
	// Mappings of broker ids to log dir descriptions, there will be one entry
	// for each broker in the request.
	Results map[int]map[string]LogDirDescription
}

// LogDirDescription contains the description of a log directory on a particular broker.
type LogDirDescription struct {
	// Error is set to a non-nil value if there was an error.
	Error error

	// ReplicaInfos.
	ReplicaInfos map[TopicPartition]ReplicaInfo
}

// TopicPartition contains a topic name and partition number.
type TopicPartition struct {
	// Name of the topic
	Topic string

	// ID of the partition.
	Partition int
}

func (tp *TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

// ReplicaInfo carries information about the description of a replica
// in a topic partition.
type ReplicaInfo struct {
	Size      int64
	OffsetLag int64
	IsFuture  bool
}

// DescribeLogDirs sends a describeLogDirs request to a kafka broker and returns the
// response.
func (c *Client) DescribeLogDirs(ctx context.Context, req *DescribeLogDirsRequest) (*DescribeLogDirsResponse, error) {
	ret := &DescribeLogDirsResponse{
		Results: make(map[int]map[string]LogDirDescription),
	}

	meta, err := c.Metadata(ctx, &MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).DescribeLogDirs: %w", err)
	}

	topics := make([]describelogdirs.RequestTopic, 0, len(meta.Topics))
	for _, topic := range meta.Topics {
		partitions := make([]int32, 0, len(topic.Partitions))
		for _, p := range topic.Partitions {
			partitions = append(partitions, int32(p.ID))
		}
		topics = append(topics, describelogdirs.RequestTopic{
			Topic:      topic.Name,
			Partitions: partitions,
		})
	}

	if req.BrokerIDs == nil {
		for _, broker := range meta.Brokers {
			req.BrokerIDs = append(req.BrokerIDs, broker.ID)
		}
	}

	for _, brokerId := range req.BrokerIDs {
		m, err := c.roundTrip(ctx, req.Addr, &describelogdirs.Request{Topics: topics, BrokerID: int32(brokerId)})
		if err != nil {
			return nil, fmt.Errorf("kafka.(*Client).DescribeLogDirs: %w", err)
		}

		res := m.(*describelogdirs.Response)

		logDirs := make(map[string]LogDirDescription, len(res.Results))
		ret.Results[brokerId] = logDirs

		for _, r := range res.Results {
			replicaInfos := make(map[TopicPartition]ReplicaInfo)
			logDirs[r.LogDir] = LogDirDescription{
				Error:        makeError(r.ErrorCode, ""),
				ReplicaInfos: replicaInfos,
			}

			for _, t := range r.Topics {
				for _, p := range t.Partitions {
					tp := TopicPartition{
						Topic:     t.Name,
						Partition: int(p.PartitionIndex),
					}
					replicaInfos[tp] = ReplicaInfo{
						Size:      p.PartitionSize,
						OffsetLag: p.OffsetLag,
						IsFuture:  p.IsFutureKey,
					}
				}
			}
		}
	}

	return ret, nil
}
