package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol/describegroups"
)

func TestClientDescribeGroups(t *testing.T) {
	if os.Getenv("KAFKA_VERSION") == "2.3.1" {
		// There's a bug in 2.3.1 that causes the MemberMetadata to be in the wrong format and thus
		// leads to an error when decoding the DescribeGroupsResponse.
		//
		// See https://issues.apache.org/jira/browse/KAFKA-9150 for details.
		t.Skip("Skipping because kafka version is 2.3.1")
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	gid := fmt.Sprintf("%s-test-group", topic)

	createTopic(t, topic, 2)
	defer deleteTopic(t, topic)

	w := newTestWriter(WriterConfig{
		Topic: topic,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := w.WriteMessages(
		ctx,
		Message{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	r := NewReader(ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topic,
		GroupID:  gid,
		MinBytes: 10,
		MaxBytes: 1000,
	})
	_, err = r.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.DescribeGroups(
		ctx,
		&DescribeGroupsRequest{
			GroupIDs: []string{gid},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Groups) != 1 {
		t.Fatal(
			"Unexpected number of groups returned",
			"expected", 1,
			"got", len(resp.Groups),
		)
	}
	g := resp.Groups[0]
	if g.Error != nil {
		t.Error(
			"Wrong error in group response",
			"expected", nil,
			"got", g.Error,
		)
	}

	if g.GroupID != gid {
		t.Error(
			"Wrong groupID",
			"expected", gid,
			"got", g.GroupID,
		)
	}

	if len(g.Members) != 1 {
		t.Fatal(
			"Wrong group members length",
			"expected", 1,
			"got", len(g.Members),
		)
	}
	if len(g.Members[0].MemberAssignments.Topics) != 1 {
		t.Fatal(
			"Wrong topics length",
			"expected", 1,
			"got", len(g.Members[0].MemberAssignments.Topics),
		)
	}
	mt := g.Members[0].MemberAssignments.Topics[0]
	if mt.Topic != topic {
		t.Error(
			"Wrong member assignment topic",
			"expected", topic,
			"got", mt.Topic,
		)
	}

	// Partitions can be in any order, sort them
	sort.Slice(mt.Partitions, func(a, b int) bool {
		return mt.Partitions[a] < mt.Partitions[b]
	})

	if !reflect.DeepEqual([]int{0, 1}, mt.Partitions) {
		t.Error(
			"Wrong member assignment partitions",
			"expected", []int{0, 1},
			"got", mt.Partitions,
		)
	}
}

// mockRoundTripper is a mock transport that returns a fixed response.
type mockRoundTripper struct {
	response Response
	err      error
}

func (m *mockRoundTripper) RoundTrip(ctx context.Context, addr net.Addr, req Request) (Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

func TestDescribeGroupsErrorHandling(t *testing.T) {
	// This test verifies that when one group has invalid metadata/assignments,
	// it doesn't prevent other groups from being returned successfully.

	// Create a mock response with multiple groups where one has invalid data.
	mockResp := &describegroups.Response{
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:    0,
				GroupID:      "valid-group-1",
				GroupState:   "Stable",
				ProtocolType: "consumer",
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:   "member-1",
						ClientID:   "client-1",
						ClientHost: "/127.0.0.1",
						// Valid metadata: version (int16) = 0, topics array length (int32) = 0, userdata length (int32) = 0
						MemberMetadata: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
						// Valid assignments: version (int16) = 0, topics array length (int32) = 0, userdata length (int32) = 0
						MemberAssignment: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
					},
				},
			},
			{
				ErrorCode:    0,
				GroupID:      "invalid-group",
				GroupState:   "Stable",
				ProtocolType: "consumer",
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:   "member-2",
						ClientID:   "client-2",
						ClientHost: "/127.0.0.1",
						// Invalid metadata - truncated data
						MemberMetadata:   []byte{0x00},
						MemberAssignment: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
					},
				},
			},
			{
				ErrorCode:    0,
				GroupID:      "valid-group-2",
				GroupState:   "Stable",
				ProtocolType: "consumer",
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:   "member-3",
						ClientID:   "client-3",
						ClientHost: "/127.0.0.1",
						// Valid metadata
						MemberMetadata: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
						// Valid assignments
						MemberAssignment: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
					},
				},
			},
		},
	}

	client := &Client{
		Transport: &mockRoundTripper{response: mockResp},
	}

	ctx := context.Background()
	resp, err := client.DescribeGroups(ctx, &DescribeGroupsRequest{
		Addr:     TCP("localhost:9092"),
		GroupIDs: []string{"valid-group-1", "invalid-group", "valid-group-2"},
	})

	if err != nil {
		t.Fatalf("Unexpected error from DescribeGroups: %v", err)
	}

	if len(resp.Groups) != 3 {
		t.Fatalf("Expected 3 groups, got %d", len(resp.Groups))
	}

	// Verify valid-group-1 has no error and has members
	if resp.Groups[0].GroupID != "valid-group-1" {
		t.Errorf("Expected first group to be valid-group-1, got %s", resp.Groups[0].GroupID)
	}
	if resp.Groups[0].Error != nil {
		t.Errorf("Expected valid-group-1 to have no error, got %v", resp.Groups[0].Error)
	}
	if len(resp.Groups[0].Members) != 1 {
		t.Errorf("Expected valid-group-1 to have 1 member, got %d", len(resp.Groups[0].Members))
	}

	// Verify invalid-group has an error and no members
	if resp.Groups[1].GroupID != "invalid-group" {
		t.Errorf("Expected second group to be invalid-group, got %s", resp.Groups[1].GroupID)
	}
	if resp.Groups[1].Error == nil {
		t.Error("Expected invalid-group to have an error, got nil")
	}
	if len(resp.Groups[1].Members) != 0 {
		t.Errorf("Expected invalid-group to have 0 members due to error, got %d", len(resp.Groups[1].Members))
	}

	// Verify valid-group-2 has no error and has members
	if resp.Groups[2].GroupID != "valid-group-2" {
		t.Errorf("Expected third group to be valid-group-2, got %s", resp.Groups[2].GroupID)
	}
	if resp.Groups[2].Error != nil {
		t.Errorf("Expected valid-group-2 to have no error, got %v", resp.Groups[2].Error)
	}
	if len(resp.Groups[2].Members) != 1 {
		t.Errorf("Expected valid-group-2 to have 1 member, got %d", len(resp.Groups[2].Members))
	}
}

func TestDescribeGroupsInvalidAssignments(t *testing.T) {
	// Test the case where assignments are invalid but metadata is valid
	mockResp := &describegroups.Response{
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:    0,
				GroupID:      "group-with-bad-assignments",
				GroupState:   "Stable",
				ProtocolType: "consumer",
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:   "member-1",
						ClientID:   "client-1",
						ClientHost: "/127.0.0.1",
						// Valid metadata
						MemberMetadata: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
						// Invalid assignments - truncated
						MemberAssignment: []byte{0x00},
					},
				},
			},
		},
	}

	client := &Client{
		Transport: &mockRoundTripper{response: mockResp},
	}

	ctx := context.Background()
	resp, err := client.DescribeGroups(ctx, &DescribeGroupsRequest{
		Addr:     TCP("localhost:9092"),
		GroupIDs: []string{"group-with-bad-assignments"},
	})

	if err != nil {
		t.Fatalf("Unexpected error from DescribeGroups: %v", err)
	}

	// Verify we got the group back with an error
	if len(resp.Groups) != 1 {
		t.Fatalf("Expected 1 group, got %d", len(resp.Groups))
	}

	if resp.Groups[0].Error == nil {
		t.Error("Expected group to have an error for invalid assignments, got nil")
	}

	if len(resp.Groups[0].Members) != 0 {
		t.Errorf("Expected group to have 0 members due to error, got %d", len(resp.Groups[0].Members))
	}
}

func TestDescribeGroupsPartialMembersCleared(t *testing.T) {
	mockResp := &describegroups.Response{
		Groups: []describegroups.ResponseGroup{
			{
				ErrorCode:    0,
				GroupID:      "multi-member-group",
				GroupState:   "Stable",
				ProtocolType: "consumer",
				Members: []describegroups.ResponseGroupMember{
					{
						MemberID:         "member-1", // valid
						ClientID:         "client-1",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
						MemberAssignment: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
					},
					{
						MemberID:         "member-2", // invalid
						ClientID:         "client-2",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00},
						MemberAssignment: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
					},
					{
						MemberID:         "member-3", // valid, but never processed
						ClientID:         "client-3",
						ClientHost:       "/127.0.0.1",
						MemberMetadata:   []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
						MemberAssignment: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
					},
				},
			},
		},
	}

	client := &Client{
		Transport: &mockRoundTripper{response: mockResp},
	}

	ctx := context.Background()
	resp, err := client.DescribeGroups(ctx, &DescribeGroupsRequest{
		Addr:     TCP("localhost:9092"),
		GroupIDs: []string{"multi-member-group"},
	})

	if err != nil {
		t.Fatalf("Unexpected error from DescribeGroups: %v", err)
	}

	if len(resp.Groups) != 1 {
		t.Fatalf("Expected 1 group, got %d", len(resp.Groups))
	}

	group := resp.Groups[0]
	if group.Error == nil {
		t.Fatal("Expected group to have an error, got nil")
	}
	if len(group.Members) != 0 {
		t.Errorf("Expected 0 members when error occurs, got %d", len(group.Members))
	}
}
