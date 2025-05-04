package kafka

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestClientSyncGroup(t *testing.T) {
	// In order to get to a sync group call we need to first
	// join a group.
	topic := makeTopic()
	client, shutdown := newLocalClient()
	client.Timeout = time.Minute
	// Although at higher api versions ClientID is nullable
	// for some reason the SyncGroup API call errors
	// when ClientID is null.
	// The Java Kafka Consumer generates a ClientID if one is not
	// present or if the provided ClientID is empty.
	client.Transport.(*Transport).ClientID = "test-client"
	defer shutdown()

	err := clientCreateTopic(client, topic, 3)
	if err != nil {
		t.Fatal(err)
	}

	groupID := makeGroupID()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	respc, err := waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     groupID,
		KeyType: CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		t.Fatal(err)
	}

	if respc.Error != nil {
		t.Fatal(err)
	}

	groupInstanceID := "group-instance-id"
	userData := "user-data"

	var rrGroupBalancer RoundRobinGroupBalancer

	req := &JoinGroupRequest{
		GroupID:          groupID,
		GroupInstanceID:  groupInstanceID,
		ProtocolType:     "consumer",
		SessionTimeout:   time.Minute,
		RebalanceTimeout: time.Minute,
		Protocols: []GroupProtocol{
			{
				Name: rrGroupBalancer.ProtocolName(),
				Metadata: GroupProtocolSubscription{
					Topics:   []string{topic},
					UserData: []byte(userData),
					OwnedPartitions: map[string][]int{
						topic: {0, 1, 2},
					},
				},
			},
		},
	}

	var resp *JoinGroupResponse

	for {
		resp, err = client.JoinGroup(ctx, req)
		if err != nil {
			t.Fatal(err)
		}

		if errors.Is(resp.Error, MemberIDRequired) {
			req.MemberID = resp.MemberID
			time.Sleep(time.Second)
			continue
		}

		if resp.Error != nil {
			t.Fatal(resp.Error)
		}
		break
	}

	if resp.MemberID != resp.LeaderID {
		t.Fatalf("expected to be group leader %s got %s", resp.MemberID, resp.LeaderID)
	}

	groupMembers := make([]GroupMember, 0, len(resp.Members))
	groupUserDataLookup := make(map[string]GroupMember)
	for _, member := range resp.Members {
		gm := GroupMember{
			ID:       member.ID,
			Topics:   member.Metadata.Topics,
			UserData: member.Metadata.UserData,
		}
		groupMembers = append(groupMembers, gm)
		groupUserDataLookup[member.ID] = gm
	}

	metaResp, err := client.Metadata(ctx, &MetadataRequest{
		Topics: []string{topic},
	})
	if err != nil {
		t.Fatal(err)
	}

	assignments := rrGroupBalancer.AssignGroups(groupMembers, metaResp.Topics[0].Partitions)

	sgRequest := &SyncGroupRequest{
		GroupID:         groupID,
		GenerationID:    resp.GenerationID,
		MemberID:        resp.MemberID,
		GroupInstanceID: groupInstanceID,
		ProtocolType:    "consumer",
		ProtocolName:    rrGroupBalancer.ProtocolName(),
	}

	for member, assignment := range assignments {
		sgRequest.Assignments = append(sgRequest.Assignments, SyncGroupRequestAssignment{
			MemberID: member,
			Assignment: GroupProtocolAssignment{
				AssignedPartitions: assignment,
				UserData:           groupUserDataLookup[member].UserData,
			},
		})
	}
	sgResp, err := client.SyncGroup(ctx, sgRequest)
	if err != nil {
		t.Fatal(err)
	}

	if sgResp.Error != nil {
		t.Fatal(sgResp.Error)
	}

	expectedAssignment := GroupProtocolAssignment{
		AssignedPartitions: map[string][]int{
			topic: {0, 1, 2},
		},
		UserData: []byte(userData),
	}

	if !reflect.DeepEqual(sgResp.Assignment, expectedAssignment) {
		t.Fatalf("\nexpected assignment to be \n%#v \ngot\n%#v", expectedAssignment, sgResp.Assignment)
	}
}

func TestSyncGroupResponseV0(t *testing.T) {
	item := syncGroupResponseV0{
		ErrorCode:         2,
		MemberAssignments: []byte(`blah`),
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found syncGroupResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}

func BenchmarkSyncGroupResponseV0(t *testing.B) {
	item := syncGroupResponseV0{
		ErrorCode:         2,
		MemberAssignments: []byte(`blah`),
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	r := bytes.NewReader(b.Bytes())
	reader := bufio.NewReader(r)
	size := b.Len()

	for i := 0; i < t.N; i++ {
		r.Seek(0, io.SeekStart)
		var found syncGroupResponseV0
		remain, err := (&found).readFrom(reader, size)
		if err != nil {
			t.Error(err)
			t.FailNow()
		}
		if remain != 0 {
			t.Errorf("expected 0 remain, got %v", remain)
			t.FailNow()
		}
	}
}
