package kafka

import (
	"log"
	"reflect"
	"testing"
	"time"
)

// todo : test more things...especially error cases.

var _ coordinator = mockCoordinator{}

type mockCoordinator struct {
	closeFunc          func() error
	joinGroupFunc      func(joinGroupRequestV1) (joinGroupResponseV1, error)
	syncGroupFunc      func(syncGroupRequestV0) (syncGroupResponseV0, error)
	leaveGroupFunc     func(leaveGroupRequestV0) (leaveGroupResponseV0, error)
	heartbeatFunc      func(heartbeatRequestV0) (heartbeatResponseV0, error)
	offsetFetchFunc    func(offsetFetchRequestV1) (offsetFetchResponseV1, error)
	offsetCommitFunc   func(offsetCommitRequestV2) (offsetCommitResponseV2, error)
	ReadPartitionsFunc func(...string) ([]Partition, error)
}

func (c mockCoordinator) Close() error {
	if c.closeFunc != nil {
		return c.closeFunc()
	}
	return nil
}

func (c mockCoordinator) joinGroup(req joinGroupRequestV1) (joinGroupResponseV1, error) {
	return c.joinGroupFunc(req)
}

func (c mockCoordinator) syncGroup(req syncGroupRequestV0) (syncGroupResponseV0, error) {
	return c.syncGroupFunc(req)
}

func (c mockCoordinator) leaveGroup(req leaveGroupRequestV0) (leaveGroupResponseV0, error) {
	return c.leaveGroupFunc(req)
}

func (c mockCoordinator) heartbeat(req heartbeatRequestV0) (heartbeatResponseV0, error) {
	return c.heartbeatFunc(req)
}

func (c mockCoordinator) offsetFetch(req offsetFetchRequestV1) (offsetFetchResponseV1, error) {
	return c.offsetFetchFunc(req)
}

func (c mockCoordinator) offsetCommit(req offsetCommitRequestV2) (offsetCommitResponseV2, error) {
	return c.offsetCommitFunc(req)
}

func (c mockCoordinator) ReadPartitions(topics ...string) ([]Partition, error) {
	return c.ReadPartitionsFunc(topics...)
}

func TestReaderAssignTopicPartitions(t *testing.T) {
	conn := &mockCoordinator{
		ReadPartitionsFunc: func(...string) ([]Partition, error) {
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
			resp.Members = append(resp.Members, joinGroupResponseMemberV1{
				MemberID: memberID,
				MemberMetadata: groupMetadata{
					Topics: topics,
				}.bytes(),
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
		ReadPartitionsFunc: func(...string) ([]Partition, error) {
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
		log:      func(func(*log.Logger)) {},
		logError: func(func(*log.Logger)) {},
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
		if time.Now().Sub(now).Seconds() > watchTime.Seconds()*4 {
			t.Error("partitionWatcher didn't see update")
		}
	}
}
