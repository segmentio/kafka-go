package describegroups

import (
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const v0 int16 = 0

func TestRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &Request{})

	prototest.TestRequest(t, v0, &Request{GroupIDs: []string{
		"g1", "g2", "g3",
	}})
}

func TestResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &Response{})

	prototest.TestResponse(t, v0, &Response{
		Groups: []Group{
			{
				ErrorCode:    0,
				GroupID:      "g1",
				State:        AwaitingSync,
				ProtocolType: "t1",
				Protocol:     "proto1",
				Members: []GroupMember{
					{
						MemberID:   "abd",
						ClientID:   "client1",
						ClientHost: "foo.bar.com",
					},
				},
			},
			{
				ErrorCode:    12,
				GroupID:      "g2",
				State:        Stable,
				ProtocolType: "t2",
				Protocol:     "proto2",
				Members: []GroupMember{
					{
						MemberID:         "abd",
						ClientID:         "c1",
						ClientHost:       "foo.bar.com",
						MemberMetadata:   []byte("stuff"),
						MemberAssignment: []byte("assigment1"),
					},
					{
						MemberID:         "def",
						ClientID:         "client2",
						ClientHost:       "foo.baz.com",
						MemberMetadata:   []byte("things"),
						MemberAssignment: []byte("assigment2"),
					},
				},
			},
		},
	})
}

func TestConsumerGroups(t *testing.T) {
	t.Run("ConsumerGroupAssignment", func(t *testing.T) {
		assignment := ConsumerGroupAssignment{
			Version: 1,
			Topics: []ConsumerGroupTopic{
				{
					Topic:      "foo",
					Partitions: []int32{1, 2, 3},
				},
				{
					Topic:      "bar",
					Partitions: []int32{4, 5, 6},
				},
			},
			UserData: []byte("stuff"),
		}

		b, err := kafka.Marshal(assignment)
		if err != nil {
			t.Fatalf("error marshalling assignment: %v", err)
		}

		var parsed ConsumerGroupAssignment
		if err := kafka.Unmarshal(b, &parsed); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(assignment, parsed) {
			t.Fatalf("mismatch after marshal/unmarshal: %v %v", assignment, parsed)
		}
	})

	t.Run("ConsumerGroupMetadata", func(t *testing.T) {
		meta := ConsumerGroupMetadata{
			Version:  1,
			Topics:   []string{"foo", "bar"},
			UserData: []byte("stuff"),
		}

		b, err := kafka.Marshal(meta)
		if err != nil {
			t.Fatalf("error marshalling metadata: %v", err)
		}

		var parsed ConsumerGroupMetadata
		if err := kafka.Unmarshal(b, &parsed); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(meta, parsed) {
			t.Fatalf("mismatch after marshal/unmarshal: %v %v", meta, parsed)
		}
	})
}
