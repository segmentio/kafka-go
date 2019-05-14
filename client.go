package kafka

import (
	"context"
	"fmt"
)

// Client is a new and experimental API for kafka-go. It is expected that this API will grow over time
// and provide a "mid-level" API. Specifically, it expected Client will be higher level than the Conn API,
// yet provide more control and lower level operations than the Reader/Writer APIs.
//
// N.B as Client is currently experimental it is subject to change, including breaking changes between MINOR and PATCH releases.
type Client struct {
	brokers []string
	dialer  *Dialer
}

type ClientConfig struct {
	Brokers []string
	Dialer  *Dialer
}

// NewClient creates and returns a *Client taking ...string of bootstrap
// brokers for connecting to the cluster.
func NewClient(brokers ...string) *Client {
	return NewClientWith(ClientConfig{Brokers: brokers, Dialer: DefaultDialer})
}

// NewClientWith creates and returns a *Client. For safety, it copies the []string of bootstrap
// brokers for connecting to the cluster and uses the user supplied Dialer.
// In the event the Dialer is nil, we use the DefaultDialer.
func NewClientWith(config ClientConfig) *Client {
	if len(config.Brokers) == 0 {
		panic("must provide at least one broker")
	}

	b := make([]string, len(config.Brokers))
	copy(b, config.Brokers)
	d := config.Dialer
	if d == nil {
		d = DefaultDialer
	}

	return &Client{
		brokers: b,
		dialer:  d,
	}
}

// ConsumerOffsets returns a map[int]int64 of partition to committed offset for a consumer group id and topic
func (c *Client) ConsumerOffsets(ctx context.Context, groupId string, topic string) (map[int]int64, error) {
	address, err := c.lookupCoordinator(groupId)
	if err != nil {
		return nil, err
	}

	conn, err := c.coordinator(ctx, address)
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

	if len(offsets.Responses) != 1 {
		return nil, fmt.Errorf("error fetching offsets, no responses received")
	}

	offsetsByPartition := map[int]int64{}
	for _, pr := range offsets.Responses[0].PartitionResponses {
		offset := pr.Offset
		if offset < 0 {
			// No offset stored
			// -1 indicates that there is no offset saved for the partition.
			// If we returned a -1 here the user might interpret that as LastOffset
			// so we set to Firstoffset for safety.
			// See http://kafka.apache.org/protocol.html#The_Messages_OffsetFetch
			offset = FirstOffset
		}
		offsetsByPartition[int(pr.Partition)] = offset
	}

	return offsetsByPartition, nil
}

// connect returns a connection to ANY broker
func (c *Client) connect() (conn *Conn, err error) {
	for _, broker := range c.brokers {
		if conn, err = c.dialer.Dial("tcp", broker); err == nil {
			return
		}
	}
	return // err will be non-nil
}

// coordinator returns a connection to a coordinator
func (c *Client) coordinator(ctx context.Context, address string) (*Conn, error) {
	conn, err := c.dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to coordinator, %v", address)
	}

	return conn, nil
}

// lookupCoordinator scans the brokers and looks up the address of the
// coordinator for the groupId.
func (c *Client) lookupCoordinator(groupId string) (string, error) {
	conn, err := c.connect()
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
