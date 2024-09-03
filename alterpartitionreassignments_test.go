package kafka

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/segmentio/kafka-go/protocol/alterpartitionreassignments"
	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientAlterPartitionReassignments(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.4.0") {
		return
	}

	ctx := context.Background()
	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 2)
	defer deleteTopic(t, topic)

	// Local kafka only has 1 broker, so any partition reassignments are really no-ops.
	resp, err := client.AlterPartitionReassignments(
		ctx,
		&AlterPartitionReassignmentsRequest{
			Topic: topic,
			Assignments: []AlterPartitionReassignmentsRequestAssignment{
				{
					PartitionID: 0,
					BrokerIDs:   []int{1},
				},
				{
					PartitionID: 1,
					BrokerIDs:   []int{1},
				},
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Error(
			"Unexpected error in response",
			"expected", nil,
			"got", resp.Error,
		)
	}
	if len(resp.PartitionResults) != 2 {
		t.Error(
			"Unexpected length of partition results",
			"expected", 2,
			"got", len(resp.PartitionResults),
		)
	}
}

func TestClientAlterPartitionReassignmentsMultiTopics(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.4.0") {
		return
	}

	ctx := context.Background()
	client, shutdown := newLocalClient()
	defer shutdown()

	topic1 := makeTopic()
	topic2 := makeTopic()
	createTopic(t, topic1, 2)
	createTopic(t, topic2, 2)
	defer func() {
		deleteTopic(t, topic1)
		deleteTopic(t, topic2)
	}()

	// Local kafka only has 1 broker, so any partition reassignments are really no-ops.
	resp, err := client.AlterPartitionReassignments(
		ctx,
		&AlterPartitionReassignmentsRequest{
			Assignments: []AlterPartitionReassignmentsRequestAssignment{
				{
					Topic:       topic1,
					PartitionID: 0,
					BrokerIDs:   []int{1},
				},
				{
					Topic:       topic1,
					PartitionID: 1,
					BrokerIDs:   []int{1},
				},
				{
					Topic:       topic2,
					PartitionID: 0,
					BrokerIDs:   []int{1},
				},
			},
		},
	)

	if err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Error(
			"Unexpected error in response",
			"expected", nil,
			"got", resp.Error,
		)
	}
	if len(resp.PartitionResults) != 3 {
		t.Error(
			"Unexpected length of partition results",
			"expected", 3,
			"got", len(resp.PartitionResults),
		)
	}
}

type mockRoundTripper struct {
	t        *testing.T
	expected alterpartitionreassignments.Request
}

var _ RoundTripper = &mockRoundTripper{}

func (m mockRoundTripper) RoundTrip(_ context.Context, _ net.Addr, req Request) (Response, error) {
	switch msg := req.(type) {
	case *alterpartitionreassignments.Request:
		require.Equal(m.t, m.expected, *msg)
	default:
		m.t.Error(
			"Unexpected req type",
			"expected", "*alterpartitionreassignments.Request",
			"got", msg,
		)
	}

	return &alterpartitionreassignments.Response{}, nil
}

func TestClientAlterPartitionReassignmentsNilReplicas(t *testing.T) {
	client := &Client{
		Addr: TCP("nohost"),
		Transport: mockRoundTripper{
			t: t,
			expected: alterpartitionreassignments.Request{
				Topics: []alterpartitionreassignments.RequestTopic{
					{
						Name: "foobar",
						Partitions: []alterpartitionreassignments.RequestPartition{
							{
								PartitionIndex: 0,
								Replicas:       nil,
							},
						},
					},
				},
			},
		},
	}

	_, err := client.AlterPartitionReassignments(context.Background(), &AlterPartitionReassignmentsRequest{
		Assignments: []AlterPartitionReassignmentsRequestAssignment{
			{
				Topic:       "foobar",
				PartitionID: 0,
				BrokerIDs:   nil,
			},
		},
	})
	require.NoError(t, err)
}
