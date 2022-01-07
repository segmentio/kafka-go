package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/createtopics"
	meta "github.com/segmentio/kafka-go/protocol/metadata"
)

func TestIssue477(t *testing.T) {
	// This test verifies that a connection attempt with a minimal TLS
	// configuration does not panic.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	cg := connGroup{
		addr: l.Addr(),
		pool: &connPool{
			dial: defaultDialer.DialContext,
			tls:  &tls.Config{},
		},
	}

	if _, err := cg.connect(context.Background(), cg.addr); err != nil {
		// An error is expected here because we are not actually establishing
		// a TLS connection to a kafka broker.
		t.Log(err)
	} else {
		t.Error("no error was reported when attempting to establish a TLS connection to a non-TLS endpoint")
	}
}

func TestIssue672(t *testing.T) {
	// ensure the test times out if the bug is re-introduced
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// we'll simulate a situation with one good topic and one bad topic (bad configuration)
	const brokenTopicName = "bad-topic"
	const okTopicName = "good-topic"

	// make the connection pool think it's immediately ready to send
	ready := make(chan struct{})
	close(ready)

	// allow the system to wake as much as it wants
	wake := make(chan event)
	defer close(wake)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-wake:
				if e == nil {
					return
				}
				e.trigger()
			}
		}
	}()

	// handle requests by immediately resolving them with a create topics response,
	// the "bad topic" will have an error value
	requests := make(chan connRequest, 1)
	defer close(requests)
	go func() {
		request := <-requests
		request.res.resolve(&createtopics.Response{
			ThrottleTimeMs: 0,
			Topics: []createtopics.ResponseTopic{
				{
					Name:         brokenTopicName,
					ErrorCode:    int16(InvalidPartitionNumber),
					ErrorMessage: InvalidPartitionNumber.Description(),
				},
				{
					Name:              okTopicName,
					NumPartitions:     1,
					ReplicationFactor: 1,
				},
			},
		})
	}()

	pool := &connPool{
		ready: ready,
		wake:  wake,
		conns: map[int32]*connGroup{},
	}

	// configure the state so it can find the good topic, but not the one that fails to create
	pool.setState(connPoolState{
		layout: protocol.Cluster{
			Topics: map[string]protocol.Topic{
				okTopicName: {
					Name: okTopicName,
					Partitions: map[int32]protocol.Partition{
						0: {},
					},
				},
			},
		},
	})

	// trick the connection pool into thinking it has a valid connection to a broker
	pool.conns[0] = &connGroup{
		pool:   pool,
		broker: Broker{},
		idleConns: []*conn{
			{
				reqs: requests,
			},
		},
	}

	// perform the round trip:
	// - if the issue is presenting this will hang waiting for metadata to arrive that will
	//   never arrive, causing a deadline timeout.
	// - if the issue is fixed this will resolve almost instantaneously
	r, err := pool.roundTrip(ctx, &createtopics.Request{
		Topics: []createtopics.RequestTopic{
			{
				Name:              brokenTopicName,
				NumPartitions:     0,
				ReplicationFactor: 1,
			},
			{
				Name:              okTopicName,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	})
	// detect if the issue is presenting using the context timeout (note that checking the err return value
	// isn't good enough as the original implementation didn't return the context cancellation error due to
	// being run in a defer)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("issue 672 is presenting! roundTrip should not have timed out")
	}

	// ancillary assertions as general house-keeping, not directly related to the issue:

	// we're not expecting any errors in this test
	if err != nil {
		t.Fatalf("unexpected error provoking connection pool roundTrip: %v", err)
	}

	// we expect a response containing the errors from the broker
	if r == nil {
		t.Fatal("expected a non-nil response")
	}

	// we expect to have the create topic response with created earlier
	_, ok := r.(*createtopics.Response)
	if !ok {
		t.Fatalf("expected a createtopics.Response but got %T", r)
	}
}

func TestIssue806(t *testing.T) {
	// ensure the test times out if the bug is re-introduced
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// simulate unknown topic want auto create with unknownTopicName,
	const unknownTopicName = "unknown-topic"
	const okTopicName = "good-topic"

	// make the connection pool think it's immediately ready to send
	ready := make(chan struct{})
	close(ready)

	// allow the system to wake as much as it wants
	wake := make(chan event)
	defer close(wake)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-wake:
				if e == nil {
					return
				}
				e.trigger()
			}
		}
	}()

	// handle requests by immediately resolving them with a create topics response,
	// the "unknown topic" will have err UNKNOWN_TOPIC_OR_PARTITION
	requests := make(chan connRequest, 1)
	defer close(requests)
	go func() {
		request := <-requests
		request.res.resolve(&meta.Response{
			Topics: []meta.ResponseTopic{
				{
					Name:      unknownTopicName,
					ErrorCode: int16(UnknownTopicOrPartition),
				},
				{
					Name: okTopicName,
					Partitions: []meta.ResponsePartition{
						{
							PartitionIndex: 0,
						},
					},
				},
			},
		})
	}()

	pool := &connPool{
		ready: ready,
		wake:  wake,
		conns: map[int32]*connGroup{},
	}

	// configure the state,
	//
	// set cached metadata only have good topic,
	// so it need to request metadata,
	// caused by unknown topic cannot find in cached metadata
	//
	// set layout only have good topic,
	// so it can find the good topic, but not the one that fails to create
	pool.setState(connPoolState{
		metadata: &meta.Response{
			Topics: []meta.ResponseTopic{
				{
					Name: okTopicName,
					Partitions: []meta.ResponsePartition{
						{
							PartitionIndex: 0,
						},
					},
				},
			},
		},
		layout: protocol.Cluster{
			Topics: map[string]protocol.Topic{
				okTopicName: {
					Name: okTopicName,
					Partitions: map[int32]protocol.Partition{
						0: {},
					},
				},
			},
		},
	})

	// trick the connection pool into thinking it has a valid connection to request metadata
	pool.ctrl = &connGroup{
		pool:   pool,
		broker: Broker{},
		idleConns: []*conn{
			{
				reqs: requests,
			},
		},
	}

	// perform the round trip:
	// - if the issue is presenting this will hang waiting for metadata to arrive that will
	//   never arrive, causing a deadline timeout.
	// - if the issue is fixed this will resolve almost instantaneously
	r, err := pool.roundTrip(ctx, &meta.Request{
		TopicNames:             []string{unknownTopicName},
		AllowAutoTopicCreation: true,
	})
	// detect if the issue is presenting using the context timeout (note that checking the err return value
	// isn't good enough as the original implementation didn't return the context cancellation error due to
	// being run in a defer)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("issue 806 is presenting! roundTrip should not have timed out")
	}

	// ancillary assertions as general house-keeping, not directly related to the issue:

	// we're not expecting any errors in this test
	if err != nil {
		t.Fatalf("unexpected error provoking connection pool roundTrip: %v", err)
	}

	// we expect a response containing the errors from the broker
	if r == nil {
		t.Fatal("expected a non-nil response")
	}

	// we expect to have the create topic response with created earlier
	_, ok := r.(*meta.Response)
	if !ok {
		t.Fatalf("expected a meta.Response but got %T", r)
	}
}
