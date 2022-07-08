package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

var _ coordinator = mockCoordinator{}

type mockCoordinator struct {
	joinGroupFunc      func(context.Context, *JoinGroupRequest) (*JoinGroupResponse, error)
	syncGroupFunc      func(context.Context, *SyncGroupRequest) (*SyncGroupResponse, error)
	leaveGroupFunc     func(context.Context, *LeaveGroupRequest) (*LeaveGroupResponse, error)
	heartbeatFunc      func(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	offsetFetchFunc    func(context.Context, *OffsetFetchRequest) (*OffsetFetchResponse, error)
	offsetCommitFunc   func(context.Context, *OffsetCommitRequest) (*OffsetCommitResponse, error)
	readPartitionsFunc func(context.Context, ...string) ([]Partition, error)
}

func (c mockCoordinator) joinGroup(ctx context.Context, req *JoinGroupRequest) (*JoinGroupResponse, error) {
	if c.joinGroupFunc == nil {
		return nil, errors.New("no joinGroup behavior specified")
	}
	return c.joinGroupFunc(ctx, req)
}

func (c mockCoordinator) syncGroup(ctx context.Context, req *SyncGroupRequest) (*SyncGroupResponse, error) {
	if c.syncGroupFunc == nil {
		return nil, errors.New("no syncGroup behavior specified")
	}
	return c.syncGroupFunc(ctx, req)
}

func (c mockCoordinator) leaveGroup(ctx context.Context, req *LeaveGroupRequest) (*LeaveGroupResponse, error) {
	if c.leaveGroupFunc == nil {
		return nil, errors.New("no leaveGroup behavior specified")
	}
	return c.leaveGroupFunc(ctx, req)
}

func (c mockCoordinator) heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	if c.heartbeatFunc == nil {
		return nil, errors.New("no heartbeat behavior specified")
	}
	return c.heartbeatFunc(ctx, req)
}

func (c mockCoordinator) offsetFetch(ctx context.Context, req *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	if c.offsetFetchFunc == nil {
		return nil, errors.New("no offsetFetch behavior specified")
	}
	return c.offsetFetchFunc(ctx, req)
}

func (c mockCoordinator) offsetCommit(ctx context.Context, req *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	if c.offsetCommitFunc == nil {
		return nil, errors.New("no offsetCommit behavior specified")
	}
	return c.offsetCommitFunc(ctx, req)
}

func (c mockCoordinator) readPartitions(ctx context.Context, topics ...string) ([]Partition, error) {
	if c.readPartitionsFunc == nil {
		return nil, errors.New("no Readpartitions behavior specified")
	}
	return c.readPartitionsFunc(ctx, topics...)
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
	coord := &mockCoordinator{
		readPartitionsFunc: func(context.Context, ...string) ([]Partition, error) {
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

	newJoinGroupResponse := func(topicsByMemberID map[string][]string) *JoinGroupResponse {
		resp := JoinGroupResponse{
			ProtocolName: RoundRobinGroupBalancer{}.ProtocolName(),
		}

		for memberID, topics := range topicsByMemberID {
			resp.Members = append(resp.Members, JoinGroupResponseMember{
				ID: memberID,
				Metadata: GroupProtocolSubscription{
					Topics: topics,
				},
			})
		}

		return &resp
	}

	testCases := map[string]struct {
		Members     *JoinGroupResponse
		Assignments GroupMemberAssignments
	}{
		"nil": {
			Members:     newJoinGroupResponse(nil),
			Assignments: GroupMemberAssignments{},
		},
		"one member, one topic": {
			Members: newJoinGroupResponse(map[string][]string{
				"member-1": {"topic-1"},
			}),
			Assignments: GroupMemberAssignments{
				"member-1": map[string][]int{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"one member, two topics": {
			Members: newJoinGroupResponse(map[string][]string{
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
			Members: newJoinGroupResponse(map[string][]string{
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
			Members: newJoinGroupResponse(map[string][]string{
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
			cg := ConsumerGroup{
				coord: coord,
			}
			cg.config.GroupBalancers = []GroupBalancer{
				RangeGroupBalancer{},
				RoundRobinGroupBalancer{},
			}
			assignments, err := cg.assignTopicPartitions(tc.Members)
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
		leaveGroupFunc: func(_ context.Context, req *LeaveGroupRequest) (*LeaveGroupResponse, error) {
			lock.Lock()
			left = append(left, req.Members[0].ID)
			lock.Unlock()
			return &LeaveGroupResponse{
				Members: []LeaveGroupResponseMember{
					{
						ID: req.Members[0].ID,
					},
				},
			}, nil
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
			scenario: "fails to join group (general error)",
			prepare: func(mc *mockCoordinator) {
				mc.joinGroupFunc = func(context.Context, *JoinGroupRequest) (*JoinGroupResponse, error) {
					return nil, errors.New("join group failed")
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
				mc.joinGroupFunc = func(context.Context, *JoinGroupRequest) (*JoinGroupResponse, error) {
					return &JoinGroupResponse{
						Error: makeError(int16(InvalidTopic), ""),
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
				mc.joinGroupFunc = func(context.Context, *JoinGroupRequest) (*JoinGroupResponse, error) {
					return &JoinGroupResponse{
						GenerationID: 12345,
						ProtocolName: "foo",
						LeaderID:     "abc",
						MemberID:     "abc",
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
				mc.joinGroupFunc = func(context.Context, *JoinGroupRequest) (*JoinGroupResponse, error) {
					return &JoinGroupResponse{
						GenerationID: 12345,
						ProtocolName: "range",
						LeaderID:     "abc",
						MemberID:     "abc",
					}, nil
				}
				mc.readPartitionsFunc = func(context.Context, ...string) ([]Partition, error) {
					return []Partition{}, nil
				}
				mc.syncGroupFunc = func(context.Context, *SyncGroupRequest) (*SyncGroupResponse, error) {
					return nil, errors.New("sync group failed")
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
				mc.syncGroupFunc = func(context.Context, *SyncGroupRequest) (*SyncGroupResponse, error) {
					return &SyncGroupResponse{
						Error: makeError(int16(InvalidTopic), ""),
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
				Logger:            &testKafkaLogger{T: t},
				coord:             mc,
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
		readPartitionsFunc: func(context.Context, ...string) ([]Partition, error) {
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
		coord:    conn,
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
		coord:    &mockCoordinator{},
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
