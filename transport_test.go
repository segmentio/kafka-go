package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/createtopics"
	meta "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
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

// TestRoundTripRefreshesMetadataOnStaleError verifies that a Produce response
// carrying a stale-metadata error code (e.g. NotLeaderForPartition) triggers an
// asynchronous metadata refresh so the next attempt can be routed to the new
// partition leader.
func TestRoundTripRefreshesMetadataOnStaleError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	const topic = "topic"

	ready := make(chan struct{})
	close(ready)

	// A buffered wake channel lets requestMetadataUpdate's non-blocking send
	// succeed without a concurrent reader, making the assertion deterministic.
	wake := make(chan event, 1)

	// Resolve the produce request with a NotLeaderForPartition error.
	requests := make(chan connRequest, 1)
	defer close(requests)
	go func() {
		request := <-requests
		request.res.resolve(&produceAPI.Response{
			Topics: []produceAPI.ResponseTopic{{
				Topic: topic,
				Partitions: []produceAPI.ResponsePartition{{
					Partition: 0,
					ErrorCode: int16(NotLeaderForPartition),
				}},
			}},
		})
	}()

	pool := &connPool{
		ready: ready,
		wake:  wake,
		conns: map[int32]*connGroup{},
	}

	pool.setState(connPoolState{
		layout: protocol.Cluster{
			Brokers: map[int32]protocol.Broker{
				0: {ID: 0},
			},
			Topics: map[string]protocol.Topic{
				topic: {
					Name: topic,
					Partitions: map[int32]protocol.Partition{
						0: {ID: 0, Leader: 0},
					},
				},
			},
		},
	})

	// Produce requests are routed to the partition leader (broker 0).
	pool.conns[0] = &connGroup{
		pool:   pool,
		broker: Broker{ID: 0},
		idleConns: []*conn{
			{
				reqs: requests,
			},
		},
	}

	r, err := pool.roundTrip(ctx, &produceAPI.Request{
		Topics: []produceAPI.RequestTopic{{
			Topic: topic,
			Partitions: []produceAPI.RequestPartition{{
				Partition: 0,
			}},
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error from roundTrip: %v", err)
	}
	if _, ok := r.(*produceAPI.Response); !ok {
		t.Fatalf("expected a produce.Response but got %T", r)
	}

	select {
	case <-wake:
		// expected: a metadata refresh was requested.
	default:
		t.Fatal("expected a metadata refresh to be requested after a stale-metadata produce error")
	}
}

// TestRequestMetadataUpdateNonBlocking verifies that requestMetadataUpdate never
// blocks the caller, even when no consumer is reading from the wake channel.
func TestRequestMetadataUpdateNonBlocking(t *testing.T) {
	pool := &connPool{
		wake: make(chan event), // unbuffered, with no reader
	}

	done := make(chan struct{})
	go func() {
		pool.requestMetadataUpdate()
		close(done)
	}()

	select {
	case <-done:
		// expected: the call returned without blocking.
	case <-time.After(time.Second):
		t.Fatal("requestMetadataUpdate blocked when no consumer was reading the wake channel")
	}
}

// TestRequestMetadataUpdateThrottled verifies that consecutive refresh requests
// are throttled: only one wake is emitted per metadataRefreshThrottle window,
// even when many callers race, bounding the load on the cluster.
func TestRequestMetadataUpdateThrottled(t *testing.T) {
	// Buffered so each accepted request is recorded without a reader.
	wake := make(chan event, 8)
	pool := &connPool{wake: wake}

	const callers = 50
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			pool.requestMetadataUpdate()
		}()
	}
	wg.Wait()

	if got := len(wake); got != 1 {
		t.Fatalf("expected a single metadata refresh within the throttle window, got %d", got)
	}

	// A second burst within the window must be throttled out.
	pool.requestMetadataUpdate()
	if got := len(wake); got != 1 {
		t.Fatalf("expected refreshes within the throttle window to be dropped, got %d", got)
	}

	// Simulating an elapsed window allows a new refresh.
	pool.lastMetadataRefresh.Store(time.Now().Add(-2 * metadataRefreshThrottle).UnixNano())
	pool.requestMetadataUpdate()
	if got := len(wake); got != 2 {
		t.Fatalf("expected a new metadata refresh after the throttle window elapsed, got %d", got)
	}
}
