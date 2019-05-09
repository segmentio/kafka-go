package kafka

import (
	"context"
	"fmt"
)

// ClusterClient provides a high-level API for miscellaneous (i.e. non consume or produce) Kafka cluster operations
type ClusterClient struct {
	brokers []string
	dialer  *Dialer
}

// NewClusterClient creates and returns a *ClusterClient with a []string of bootstrap
// brokers for connecting to the cluster and the DefaultDialer
func NewClusterClient(brokers []string) *ClusterClient {
	return &ClusterClient{
		brokers: brokers,
		dialer:  DefaultDialer,
	}
}

// NewClusterClient creates and returns a *ClusterClient with a []string of bootstrap
// brokers for connecting to the cluster and a user supplied Dialer
func NewClusterClientWith(brokers []string, d *Dialer) *ClusterClient {
	return &ClusterClient{
		brokers: brokers,
		dialer:  d,
	}
}

// ConsumerOffsets returns a map[int]int64 of partition to committed offset for a consumer group id and topic
func (cc *ClusterClient) ConsumerOffsets(ctx context.Context, groupId string, topic string) (map[int]int64, error) {
	address, err := cc.lookupCoordinator(groupId)
	if err != nil {
		return nil, err
	}

	conn, err := cc.coordinator(ctx, address)
	if err != nil {
		return nil, err
	}

	defer conn.Close()
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, err
	}

	var parts []int32
	for _, p := range partitions {
		parts = append(parts, int32(p.ID))
	}

	offsets, err := conn.offsetFetch(offsetFetchRequestV1{
		GroupID: groupId,
		Topics: []offsetFetchRequestV1Topic{
			{
				Topic:      topic,
				Partitions: parts,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	offsetsByPartition := map[int]int64{}
	for _, pr := range offsets.Responses[0].PartitionResponses {
		offset := pr.Offset
		if offset < 0 {
			// No offset stored
			offset = FirstOffset
		}
		offsetsByPartition[int(pr.Partition)] = offset
	}

	return offsetsByPartition, nil
}

// connect returns a connection to ANY broker
func (cc *ClusterClient) connect() (conn *Conn, err error) {
	for _, broker := range cc.brokers {
		if conn, err = cc.dialer.Dial("tcp", broker); err == nil {
			return
		}
	}
	return // err will be non-nil
}

// coordinator returns a connection to a coordinator
func (cc *ClusterClient) coordinator(ctx context.Context, address string) (*Conn, error) {
	conn, err := cc.dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to coordinator, %v", address)
	}

	return conn, nil
}

// lookupCoordinator scans the brokers and looks up the address of the
// coordinator for the groupId.
func (cc *ClusterClient) lookupCoordinator(groupId string) (string, error) {
	conn, err := cc.connect()
	if err != nil {
		return "", fmt.Errorf("unable to find coordinator to any connect for group, %v: %v\n", groupId, err)
	}
	defer conn.Close()

	out, err := conn.findCoordinator(findCoordinatorRequestV0{
		CoordinatorKey: groupId,
	})
	if err != nil {
		return "", fmt.Errorf("unable to find coordinator for group, %v: %v", groupId, err)
	}

	address := fmt.Sprintf("%v:%v", out.Coordinator.Host, out.Coordinator.Port)
	return address, nil
}
