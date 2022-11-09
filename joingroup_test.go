package kafka

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientJoinGroup(t *testing.T) {
	topic := makeTopic()
	client, shutdown := newLocalClient()
	defer shutdown()

	err := clientCreateTopic(client, topic, 3)
	if err != nil {
		t.Fatal(err)
	}

	groupID := makeGroupID()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
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
	if !ktesting.KafkaIsAtLeast("2.4.1") {
		groupInstanceID = ""
	}
	const userData = "user-data"

	req := &JoinGroupRequest{
		GroupID:          groupID,
		GroupInstanceID:  groupInstanceID,
		ProtocolType:     "consumer",
		SessionTimeout:   time.Minute,
		RebalanceTimeout: time.Minute,
		Protocols: []GroupProtocol{
			{
				Name: RoundRobinGroupBalancer{}.ProtocolName(),
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

	if resp.GenerationID != 1 {
		t.Fatalf("expected generation ID to be 1 but got %v", resp.GenerationID)
	}

	if resp.MemberID == "" {
		t.Fatal("expected a member ID in response")
	}

	if resp.LeaderID != resp.MemberID {
		t.Fatalf("expected to be group leader but got %v", resp.LeaderID)
	}

	if len(resp.Members) != 1 {
		t.Fatalf("expected 1 member got %v", resp.Members)
	}

	member := resp.Members[0]

	if member.ID != resp.MemberID {
		t.Fatal("expected to be the only group memmber")
	}

	if member.GroupInstanceID != groupInstanceID {
		t.Fatalf("expected the group instance ID to be %v, got %v", groupInstanceID, member.GroupInstanceID)
	}

	expectedMetadata := GroupProtocolSubscription{
		Topics:   []string{topic},
		UserData: []byte(userData),
		OwnedPartitions: map[string][]int{
			topic: {0, 1, 2},
		},
	}

	if !reflect.DeepEqual(member.Metadata, expectedMetadata) {
		t.Fatalf("\nexpected assignment to be \n%v\nbut got\n%v", expectedMetadata, member.Metadata)
	}
}

func TestSaramaCompatibility(t *testing.T) {
	var (
		// sample data from github.com/Shopify/sarama
		//
		// See consumer_group_members_test.go
		//
		groupMemberMetadata = []byte{
			0, 1, // Version
			0, 0, 0, 2, // Topic array length
			0, 3, 'o', 'n', 'e', // Topic one
			0, 3, 't', 'w', 'o', // Topic two
			0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
		}
		groupMemberAssignment = []byte{
			0, 1, // Version
			0, 0, 0, 1, // Topic array length
			0, 3, 'o', 'n', 'e', // Topic one
			0, 0, 0, 3, // Topic one, partition array length
			0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 4, // 0, 2, 4
			0, 0, 0, 3, 0x01, 0x02, 0x03, // Userdata
		}
	)

	t.Run("verify metadata", func(t *testing.T) {
		var item groupMetadata
		remain, err := (&item).readFrom(bufio.NewReader(bytes.NewReader(groupMemberMetadata)), len(groupMemberMetadata))
		if err != nil {
			t.Fatalf("bad err: %v", err)
		}
		if remain != 0 {
			t.Fatalf("expected 0; got %v", remain)
		}

		if v := item.Version; v != 1 {
			t.Errorf("expected Version 1; got %v", v)
		}
		if v := item.Topics; !reflect.DeepEqual([]string{"one", "two"}, v) {
			t.Errorf(`expected {"one", "two"}; got %v`, v)
		}
		if v := item.UserData; !reflect.DeepEqual([]byte{0x01, 0x02, 0x03}, v) {
			t.Errorf("expected []byte{0x01, 0x02, 0x03}; got %v", v)
		}
	})

	t.Run("verify assignments", func(t *testing.T) {
		var item groupAssignment
		remain, err := (&item).readFrom(bufio.NewReader(bytes.NewReader(groupMemberAssignment)), len(groupMemberAssignment))
		if err != nil {
			t.Fatalf("bad err: %v", err)
		}
		if remain != 0 {
			t.Fatalf("expected 0; got %v", remain)
		}

		if v := item.Version; v != 1 {
			t.Errorf("expected Version 1; got %v", v)
		}
		if v := item.Topics; !reflect.DeepEqual(map[string][]int32{"one": {0, 2, 4}}, v) {
			t.Errorf(`expected map[string][]int32{"one": {0, 2, 4}}; got %v`, v)
		}
		if v := item.UserData; !reflect.DeepEqual([]byte{0x01, 0x02, 0x03}, v) {
			t.Errorf("expected []byte{0x01, 0x02, 0x03}; got %v", v)
		}
	})
}

func TestMemberMetadata(t *testing.T) {
	item := groupMetadata{
		Version:  1,
		Topics:   []string{"a", "b"},
		UserData: []byte(`blah`),
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found groupMetadata
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

func TestJoinGroupResponseV1(t *testing.T) {
	item := joinGroupResponseV1{
		ErrorCode:     2,
		GenerationID:  3,
		GroupProtocol: "a",
		LeaderID:      "b",
		MemberID:      "c",
		Members: []joinGroupResponseMemberV1{
			{
				MemberID:       "d",
				MemberMetadata: []byte("blah"),
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found joinGroupResponseV1
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
