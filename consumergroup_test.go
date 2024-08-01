package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/consumer"
)

var _ coordinator = mockCoordinator{}

type mockCoordinator struct {
	closeFunc           func() error
	findCoordinatorFunc func(findCoordinatorRequestV0) (findCoordinatorResponseV0, error)
	joinGroupFunc       func(joinGroupRequestV1) (joinGroupResponseV1, error)
	syncGroupFunc       func(syncGroupRequestV0) (syncGroupResponseV0, error)
	leaveGroupFunc      func(leaveGroupRequestV0) (leaveGroupResponseV0, error)
	heartbeatFunc       func(heartbeatRequestV0) (heartbeatResponseV0, error)
	offsetFetchFunc     func(offsetFetchRequestV1) (offsetFetchResponseV1, error)
	offsetCommitFunc    func(offsetCommitRequestV2) (offsetCommitResponseV2, error)
	readPartitionsFunc  func(...string) ([]Partition, error)
}

func (c mockCoordinator) Close() error {
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return nil
}

func (c mockCoordinator) findCoordinator(req findCoordinatorRequestV0) (findCoordinatorResponseV0, error) {
	if c.findCoordinatorFunc == nil {
		return findCoordinatorResponseV0{}, errors.New("no findCoordinator behavior specified")
	}
	return c.findCoordinatorFunc(req)
}

func (c mockCoordinator) joinGroup(req joinGroupRequestV1) (joinGroupResponseV1, error) {
	if c.joinGroupFunc == nil {
		return joinGroupResponseV1{}, errors.New("no joinGroup behavior specified")
	}
	return c.joinGroupFunc(req)
}

func (c mockCoordinator) syncGroup(req syncGroupRequestV0) (syncGroupResponseV0, error) {
	if c.syncGroupFunc == nil {
		return syncGroupResponseV0{}, errors.New("no syncGroup behavior specified")
	}
	return c.syncGroupFunc(req)
}

func (c mockCoordinator) leaveGroup(req leaveGroupRequestV0) (leaveGroupResponseV0, error) {
	if c.leaveGroupFunc == nil {
		return leaveGroupResponseV0{}, errors.New("no leaveGroup behavior specified")
	}
	return c.leaveGroupFunc(req)
}

func (c mockCoordinator) heartbeat(req heartbeatRequestV0) (heartbeatResponseV0, error) {
	if c.heartbeatFunc == nil {
		return heartbeatResponseV0{}, errors.New("no heartbeat behavior specified")
	}
	return c.heartbeatFunc(req)
}

func (c mockCoordinator) offsetFetch(req offsetFetchRequestV1) (offsetFetchResponseV1, error) {
	if c.offsetFetchFunc == nil {
		return offsetFetchResponseV1{}, errors.New("no offsetFetch behavior specified")
	}
	return c.offsetFetchFunc(req)
}

func (c mockCoordinator) offsetCommit(req offsetCommitRequestV2) (offsetCommitResponseV2, error) {
	if c.offsetCommitFunc == nil {
		return offsetCommitResponseV2{}, errors.New("no offsetCommit behavior specified")
	}
	return c.offsetCommitFunc(req)
}

func (c mockCoordinator) readPartitions(topics ...string) ([]Partition, error) {
	if c.readPartitionsFunc == nil {
		return nil, errors.New("no Readpartitions behavior specified")
	}
	return c.readPartitionsFunc(topics...)
}

func TestValidateConsumerGroupConfig(t *testing.T) {
	tests := []struct {
		config       ConsumerGroupConfig
		errorOccured bool
	}{
		{config: ConsumerGroupConfig{}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, HeartbeatInterval: 2}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: -1}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", SessionTimeout: -1}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: 2, SessionTimeout: -1}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: -2}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: -1}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: 1, StartOffset: 123}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: 1, PartitionWatchInterval: -1}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: 1, PartitionWatchInterval: 1, JoinGroupBackoff: -1}, errorOccured: true},
		{config: ConsumerGroupConfig{Brokers: []string{"broker1"}, Topics: []string{"t1"}, ID: "group1", HeartbeatInterval: 2, SessionTimeout: 2, RebalanceTimeout: 2, RetentionTime: 1, PartitionWatchInterval: 1, JoinGroupBackoff: 1}, errorOccured: false},
	}
	for _, test := range tests {
		err := test.config.Validate()
		if test.errorOccured && err == nil {
			t.Error("expected an error", test.config)
		}
		if !test.errorOccured && err != nil {
			t.Error("expected no error, got", err, test.config)
		}
	}
}

func TestReaderAssignTopicPartitions(t *testing.T) {
	conn := &mockCoordinator{
		readPartitionsFunc: func(...string) ([]Partition, error) {
			return []Partition{
				{
					Topic: "topic-1",
					ID:    0,
				},
				{
					Topic: "topic-1",
					ID:    1,
				},
				{
					Topic: "topic-1",
					ID:    2,
				},
				{
					Topic: "topic-2",
					ID:    0,
				},
			}, nil
		},
	}

	newJoinGroupResponseV1 := func(topicsByMemberID map[string][]string) joinGroupResponseV1 {
		resp := joinGroupResponseV1{
			GroupProtocol: RoundRobinGroupBalancer{}.ProtocolName(),
		}

		for memberID, topics := range topicsByMemberID {
			mm, err := protocol.Marshal(1, consumer.Subscription{
				Topics: topics,
			})
			if err != nil {
				t.Errorf("error marshaling consumer subscription: %v", err)
			}
			resp.Members = append(resp.Members, joinGroupResponseMemberV1{
				MemberID:       memberID,
				MemberMetadata: mm,
			})
		}

		return resp
	}

	testCases := map[string]struct {
		Members     joinGroupResponseV1
		Assignments GroupMemberAssignments
	}{
		"nil": {
			Members:     newJoinGroupResponseV1(nil),
			Assignments: GroupMemberAssignments{},
		},
		"one member, one topic": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"one member, two topics": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1", "topic-2"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 1, 2},
					"topic-2": {0},
				},
			},
		},
		"two members, one topic": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1"},
				"member-2": {"topic-1"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 2},
				},
				"member-2": map[string][]int{
					"topic-1": {1},
				},
			},
		},
		"two members, two unshared topics": {
			Members: newJoinGroupResponseV1(map[string][]string{
				"member-1": {"topic-1"},
				"member-2": {"topic-2"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 1, 2},
				},
				"member-2": map[string][]int{
					"topic-2": {0},
				},
			},
		},
	}

	for label, tc := range testCases {
		t.Run(label, func(t *testing.T) {
			cg := ConsumerGroup{}
			cg.config.GroupBalancers = []GroupBalancer{
				RangeGroupBalancer{},
				RoundRobinGroupBalancer{},
			}
			assignments, err := cg.assignTopicPartitions(conn, tc.Members)
			if err != nil {
				t.Fatalf("bad err: %v", err)
			}
			if !reflect.DeepEqual(tc.Assignments, assignments) {
				t.Errorf("expected %v; got %v", tc.Assignments, assignments)
			}
		})
	}
}

func TestConsumerGroup(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *ConsumerGroup)
	}{
		{
			scenario: "Next returns generations",
			function: func(t *testing.T, ctx context.Context, cg *ConsumerGroup) {
				gen1, err := cg.Next(ctx)
				if gen1 == nil {
					t.Fatalf("expected generation 1 not to be nil")
				}
				if err != nil {
					t.Fatalf("expected no error, but got %+v", err)
				}
				// returning from this function should cause the generation to
				// exit.
				gen1.Start(func(context.Context) {})

				// if this fails due to context timeout, it would indicate that
				// the
				gen2, err := cg.Next(ctx)
				if gen2 == nil {
					t.Fatalf("expected generation 2 not to be nil")
				}
				if err != nil {
					t.Fatalf("expected no error, but got %+v", err)
				}

				if gen1.ID == gen2.ID {
					t.Errorf("generation ID should have changed, but it stayed as %d", gen1.ID)
				}
				if gen1.GroupID != gen2.GroupID {
					t.Errorf("mismatched group ID between generations: %s and %s", gen1.GroupID, gen2.GroupID)
				}
				if gen1.MemberID != gen2.MemberID {
					t.Errorf("mismatched member ID between generations: %s and %s", gen1.MemberID, gen2.MemberID)
				}
			},
		},

		{
			scenario: "Next returns ctx.Err() on canceled context",
			function: func(t *testing.T, _ context.Context, cg *ConsumerGroup) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				gen, err := cg.Next(ctx)
				if gen != nil {
					t.Errorf("expected generation to be nil")
				}
				if !errors.Is(err, context.Canceled) {
					t.Errorf("expected context.Canceled, but got %+v", err)
				}
			},
		},

		{
			scenario: "Next returns ErrGroupClosed on closed group",
			function: func(t *testing.T, ctx context.Context, cg *ConsumerGroup) {
				if err := cg.Close(); err != nil {
					t.Fatal(err)
				}
				gen, err := cg.Next(ctx)
				if gen != nil {
					t.Errorf("expected generation to be nil")
				}
				if !errors.Is(err, ErrGroupClosed) {
					t.Errorf("expected ErrGroupClosed, but got %+v", err)
				}
			},
		},
	}

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			group, err := NewConsumerGroup(ConsumerGroupConfig{
				ID:                makeGroupID(),
				Topics:            []string{topic},
				Brokers:           []string{"localhost:9092"},
				HeartbeatInterval: 2 * time.Second,
				RebalanceTimeout:  2 * time.Second,
				RetentionTime:     time.Hour,
				Logger:            &testKafkaLogger{T: t},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer group.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			test.function(t, ctx, group)
		})
	}
}

func TestConsumerGroupErrors(t *testing.T) {
	var left []string
	var lock sync.Mutex
	mc := mockCoordinator{
		leaveGroupFunc: func(req leaveGroupRequestV0) (leaveGroupResponseV0, error) {
			lock.Lock()
			left = append(left, req.MemberID)
			lock.Unlock()
			return leaveGroupResponseV0{}, nil
		},
	}
	assertLeftGroup := func(t *testing.T, memberID string) {
		lock.Lock()
		if !reflect.DeepEqual(left, []string{memberID}) {
			t.Errorf("expected abc to have left group once, members left: %v", left)
		}
		left = left[0:0]
		lock.Unlock()
	}

	// NOTE : the mocked behavior is accumulated across the tests, so they are
	// 		  NOT run in parallel.  this simplifies test setup so that each test
	// 	 	  can specify only the error behavior required and leverage setup
	//        from previous steps.
	tests := []struct {
		scenario string
		prepare  func(*mockCoordinator)
		function func(*testing.T, context.Context, *ConsumerGroup)
	}{
		{
			scenario: "fails to find coordinator (general error)",
			prepare: func(mc *mockCoordinator) {
				mc.findCoordinatorFunc = func(findCoordinatorRequestV0) (findCoordinatorResponseV0, error) {
					return findCoordinatorResponseV0{}, errors.New("dial error")
				}
			},
			function: func(t *testing.T, ctx context.Context, group *ConsumerGroup) {
				gen, err := group.Next(ctx)
				if err == nil {
					t.Errorf("expected an error")
				} else if err.Error() != "dial error" {
					t.Errorf("got wrong error: %+v", err)
				}
				if gen != nil {
					t.Error("expected a nil consumer group generation")
				}
			},
		},

		{
			scenario: "fails to find coordinator (error code in response)",
			prepare: func(mc *mockCoordinator) {
				mc.findCoordinatorFunc = func(findCoordinatorRequestV0) (findCoordinatorResponseV0, error) {
					return findCoordinatorResponseV0{
						ErrorCode: int16(NotCoordinatorForGroup),
					}, nil
				}
			},
			function: func(t *testing.T, ctx context.Context, group *ConsumerGroup) {
				gen, err := group.Next(ctx)
				if err == nil {
					t.Errorf("expected an error")
				} else if !errors.Is(err, NotCoordinatorForGroup) {
					t.Errorf("got wrong error: %+v", err)
				}
				if gen != nil {
					t.Error("expected a nil consumer group generation")
				}
			},
		},

		{
			scenario: "fails to join group (general error)",
			prepare: func(mc *mockCoordinator) {
				mc.findCoordinatorFunc = func(findCoordinatorRequestV0) (findCoordinatorResponseV0, error) {
					return findCoordinatorResponseV0{
						Coordinator: findCoordinatorResponseCoordinatorV0{
							NodeID: 1,
							Host:   "foo.bar.com",
							Port:   12345,
						},
					}, nil
				}
				mc.joinGroupFunc = func(joinGroupRequestV1) (joinGroupResponseV1, error) {
					return joinGroupResponseV1{}, errors.New("join group failed")
				}
				// NOTE : no stub for leaving the group b/c the member never joined.
			},
			function: func(t *testing.T, ctx context.Context, group *ConsumerGroup) {
				gen, err := group.Next(ctx)
				if err == nil {
					t.Errorf("expected an error")
				} else if err.Error() != "join group failed" {
					t.Errorf("got wrong error: %+v", err)
				}
				if gen != nil {
					t.Error("expected a nil consumer group generation")
				}
			},
		},

		{
			scenario: "fails to join group (error code)",
			prepare: func(mc *mockCoordinator) {
				mc.findCoordinatorFunc = func(findCoordinatorRequestV0) (findCoordinatorResponseV0, error) {
					return findCoordinatorResponseV0{
						Coordinator: findCoordinatorResponseCoordinatorV0{
							NodeID: 1,
							Host:   "foo.bar.com",
							Port:   12345,
						},
					}, nil
				}
				mc.joinGroupFunc = func(joinGroupRequestV1) (joinGroupResponseV1, error) {
					return joinGroupResponseV1{
						ErrorCode: int16(InvalidTopic),
					}, nil
				}
				// NOTE : no stub for leaving the group b/c the member never joined.
			},
			function: func(t *testing.T, ctx context.Context, group *ConsumerGroup) {
				gen, err := group.Next(ctx)
				if err == nil {
					t.Errorf("expected an error")
				} else if !errors.Is(err, InvalidTopic) {
					t.Errorf("got wrong error: %+v", err)
				}
				if gen != nil {
					t.Error("expected a nil consumer group generation")
				}
			},
		},

		{
			scenario: "fails to join group (leader, unsupported protocol)",
			prepare: func(mc *mockCoordinator) {
				mc.joinGroupFunc = func(joinGroupRequestV1) (joinGroupResponseV1, error) {
					return joinGroupResponseV1{
						GenerationID:  12345,
						GroupProtocol: "foo",
						LeaderID:      "abc",
						MemberID:      "abc",
					}, nil
				}
			},
			function: func(t *testing.T, ctx context.Context, group *ConsumerGroup) {
				gen, err := group.Next(ctx)
				if err == nil {
					t.Errorf("expected an error")
				} else if !strings.HasPrefix(err.Error(), "unable to find selected balancer") {
					t.Errorf("got wrong error: %+v", err)
				}
				if gen != nil {
					t.Error("expected a nil consumer group generation")
				}
				assertLeftGroup(t, "abc")
			},
		},

		{
			scenario: "fails to sync group (general error)",
			prepare: func(mc *mockCoordinator) {
				mc.joinGroupFunc = func(joinGroupRequestV1) (joinGroupResponseV1, error) {
					return joinGroupResponseV1{
						GenerationID:  12345,
						GroupProtocol: "range",
						LeaderID:      "abc",
						MemberID:      "abc",
					}, nil
				}
				mc.readPartitionsFunc = func(...string) ([]Partition, error) {
					return []Partition{}, nil
				}
				mc.syncGroupFunc = func(syncGroupRequestV0) (syncGroupResponseV0, error) {
					return syncGroupResponseV0{}, errors.New("sync group failed")
				}
			},
			function: func(t *testing.T, ctx context.Context, group *ConsumerGroup) {
				gen, err := group.Next(ctx)
				if err == nil {
					t.Errorf("expected an error")
				} else if err.Error() != "sync group failed" {
					t.Errorf("got wrong error: %+v", err)
				}
				if gen != nil {
					t.Error("expected a nil consumer group generation")
				}
				assertLeftGroup(t, "abc")
			},
		},

		{
			scenario: "fails to sync group (error code)",
			prepare: func(mc *mockCoordinator) {
				mc.syncGroupFunc = func(syncGroupRequestV0) (syncGroupResponseV0, error) {
					return syncGroupResponseV0{
						ErrorCode: int16(InvalidTopic),
					}, nil
				}
			},
			function: func(t *testing.T, ctx context.Context, group *ConsumerGroup) {
				gen, err := group.Next(ctx)
				if err == nil {
					t.Errorf("expected an error")
				} else if !errors.Is(err, InvalidTopic) {
					t.Errorf("got wrong error: %+v", err)
				}
				if gen != nil {
					t.Error("expected a nil consumer group generation")
				}
				assertLeftGroup(t, "abc")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			tt.prepare(&mc)

			group, err := NewConsumerGroup(ConsumerGroupConfig{
				ID:                makeGroupID(),
				Topics:            []string{"test"},
				Brokers:           []string{"no-such-broker"}, // should not attempt to actually dial anything
				HeartbeatInterval: 2 * time.Second,
				RebalanceTimeout:  time.Second,
				JoinGroupBackoff:  time.Second,
				RetentionTime:     time.Hour,
				connect: func(*Dialer, ...string) (coordinator, error) {
					return mc, nil
				},
				Logger: &testKafkaLogger{T: t},
			})
			if err != nil {
				t.Fatal(err)
			}

			// these tests should all execute fairly quickly since they're
			// mocking the coordinator.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			tt.function(t, ctx, group)

			if err := group.Close(); err != nil {
				t.Errorf("error on close: %+v", err)
			}
		})
	}
}

// todo : test for multi-topic?

func TestGenerationExitsOnPartitionChange(t *testing.T) {
	var count int
	partitions := [][]Partition{
		{
			Partition{
				Topic: "topic-1",
				ID:    0,
			},
		},
		{
			Partition{
				Topic: "topic-1",
				ID:    0,
			},
			{
				Topic: "topic-1",
				ID:    1,
			},
		},
	}

	conn := mockCoordinator{
		readPartitionsFunc: func(...string) ([]Partition, error) {
			p := partitions[count]
			// cap the count at len(partitions) -1 so ReadPartitions doesn't even go out of bounds
			// and long running tests don't fail
			if count < len(partitions) {
				count++
			}
			return p, nil
		},
	}

	// Sadly this test is time based, so at the end will be seeing if the runGroup run to completion within the
	// allotted time. The allotted time is 4x the PartitionWatchInterval.
	now := time.Now()
	watchTime := 500 * time.Millisecond

	gen := Generation{
		conn:     conn,
		done:     make(chan struct{}),
		joined:   make(chan struct{}),
		log:      func(func(Logger)) {},
		logError: func(func(Logger)) {},
	}

	done := make(chan struct{})
	go func() {
		gen.partitionWatcher(watchTime, "topic-1")
		close(done)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for partition watcher to exit")
	case <-done:
		if time.Since(now).Seconds() > watchTime.Seconds()*4 {
			t.Error("partitionWatcher didn't see update")
		}
	}
}

func TestGenerationStartsFunctionAfterClosed(t *testing.T) {
	gen := Generation{
		conn:     &mockCoordinator{},
		done:     make(chan struct{}),
		joined:   make(chan struct{}),
		log:      func(func(Logger)) {},
		logError: func(func(Logger)) {},
	}

	gen.close()

	ch := make(chan error)
	gen.Start(func(ctx context.Context) {
		<-ctx.Done()
		ch <- ctx.Err()
	})

	select {
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for func to run")
	case err := <-ch:
		if !errors.Is(err, ErrGenerationEnded) {
			t.Fatalf("expected %v but got %v", ErrGenerationEnded, err)
		}
	}
}
