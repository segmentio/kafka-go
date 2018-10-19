package kafka

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
)

func TestFindMembersByTopic(t *testing.T) {
	a1 := memberGroupMetadata{
		MemberID: "a",
		Metadata: groupMetadata{
			Topics: []string{"topic-1"},
		},
	}
	a12 := memberGroupMetadata{
		MemberID: "a",
		Metadata: groupMetadata{
			Topics: []string{"topic-1", "topic-2"},
		},
	}
	b23 := memberGroupMetadata{
		MemberID: "b",
		Metadata: groupMetadata{
			Topics: []string{"topic-2", "topic-3"},
		},
	}

	tests := map[string]struct {
		Members  []memberGroupMetadata
		Expected map[string][]memberGroupMetadata
	}{
		"empty": {
			Expected: map[string][]memberGroupMetadata{},
		},
		"one member, one topic": {
			Members: []memberGroupMetadata{a1},
			Expected: map[string][]memberGroupMetadata{
				"topic-1": {
					a1,
				},
			},
		},
		"one member, multiple topics": {
			Members: []memberGroupMetadata{a12},
			Expected: map[string][]memberGroupMetadata{
				"topic-1": {
					a12,
				},
				"topic-2": {
					a12,
				},
			},
		},
		"multiple members, multiple topics": {
			Members: []memberGroupMetadata{a12, b23},
			Expected: map[string][]memberGroupMetadata{
				"topic-1": {
					a12,
				},
				"topic-2": {
					a12,
					b23,
				},
				"topic-3": {
					b23,
				},
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			membersByTopic := findMembersByTopic(test.Members)
			if !reflect.DeepEqual(test.Expected, membersByTopic) {
				t.Errorf("expected %#v; got %#v", test.Expected, membersByTopic)
			}
		})
	}
}

func TestRangeAssignGroups(t *testing.T) {
	newMeta := func(memberID string, topics ...string) memberGroupMetadata {
		return memberGroupMetadata{
			MemberID: memberID,
			Metadata: groupMetadata{
				Topics: topics,
			},
		}
	}

	newPartitions := func(partitionCount int, topics ...string) []Partition {
		partitions := make([]Partition, 0, len(topics)*partitionCount)
		for _, topic := range topics {
			for partition := 0; partition < partitionCount; partition++ {
				partitions = append(partitions, Partition{
					Topic: topic,
					ID:    partition,
				})
			}
		}
		return partitions
	}

	tests := map[string]struct {
		Members    []memberGroupMetadata
		Partitions []Partition
		Expected   memberGroupAssignments
	}{
		"empty": {
			Expected: memberGroupAssignments{},
		},
		"one member, one topic, one partition": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0},
				},
			},
		},
		"one member, one topic, multiple partitions": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
			},
			Partitions: newPartitions(3, "topic-1"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"multiple members, one topic, one partition": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
				newMeta("b", "topic-1"),
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{},
				"b": map[string][]int32{
					"topic-1": {0},
				},
			},
		},
		"multiple members, one topic, multiple partitions": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
				newMeta("b", "topic-1"),
			},
			Partitions: newPartitions(3, "topic-1"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0},
				},
				"b": map[string][]int32{
					"topic-1": {1, 2},
				},
			},
		},
		"multiple members, multiple topics, multiple partitions": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1", "topic-2"),
				newMeta("b", "topic-2", "topic-3"),
			},
			Partitions: newPartitions(3, "topic-1", "topic-2", "topic-3"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0, 1, 2},
					"topic-2": {0},
				},
				"b": map[string][]int32{
					"topic-2": {1, 2},
					"topic-3": {0, 1, 2},
				},
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			assignments := rangeStrategy{}.AssignGroups(test.Members, test.Partitions)
			if !reflect.DeepEqual(test.Expected, assignments) {
				buf := bytes.NewBuffer(nil)
				encoder := json.NewEncoder(buf)
				encoder.SetIndent("", "  ")

				buf.WriteString("expected: ")
				encoder.Encode(test.Expected)
				buf.WriteString("got: ")
				encoder.Encode(assignments)

				t.Error(buf.String())
			}
		})
	}
}

// For 66 members, 213 partitions, each member should get 213/66 = 3.22 partitions.
// This means that in practice, each member should get either 3 or 4 partitions
// assigned to it. Any other number is a failure.
func TestRangeAssignGroupsUnbalanced(t *testing.T) {
	members := []memberGroupMetadata{}
	for i := 0; i < 66; i++ {
		members = append(members, memberGroupMetadata{
			MemberID: strconv.Itoa(i),
			Metadata: groupMetadata{
				Topics: []string{"topic-1"},
			},
		})
	}
	partitions := []Partition{}
	for i := 0; i < 213; i++ {
		partitions = append(partitions, Partition{
			ID:    i,
			Topic: "topic-1",
		})
	}

	assignments := rangeStrategy{}.AssignGroups(members, partitions)
	if len(assignments) != len(members) {
		t.Fatalf("Assignment count mismatch: %d != %d", len(assignments), len(members))
	}

	for _, m := range assignments {
		if len(m["topic-1"]) < 3 || len(m["topic-1"]) > 4 {
			t.Fatalf("Expected assignment of 3 or 4 partitions, got %d", len(m["topic-1"]))
		}
	}
}

func TestRoundRobinAssignGroups(t *testing.T) {
	newMeta := func(memberID string, topics ...string) memberGroupMetadata {
		return memberGroupMetadata{
			MemberID: memberID,
			Metadata: groupMetadata{
				Topics: topics,
			},
		}
	}

	newPartitions := func(partitionCount int, topics ...string) []Partition {
		partitions := make([]Partition, 0, len(topics)*partitionCount)
		for _, topic := range topics {
			for partition := 0; partition < partitionCount; partition++ {
				partitions = append(partitions, Partition{
					Topic: topic,
					ID:    partition,
				})
			}
		}
		return partitions
	}

	tests := map[string]struct {
		Members    []memberGroupMetadata
		Partitions []Partition
		Expected   memberGroupAssignments
	}{
		"empty": {
			Expected: memberGroupAssignments{},
		},
		"one member, one topic, one partition": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0},
				},
			},
		},
		"one member, one topic, multiple partitions": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
			},
			Partitions: newPartitions(3, "topic-1"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"multiple members, one topic, one partition": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1"),
				newMeta("b", "topic-1"),
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0},
				},
				"b": map[string][]int32{},
			},
		},
		"multiple members, multiple topics, multiple partitions": {
			Members: []memberGroupMetadata{
				newMeta("a", "topic-1", "topic-2"),
				newMeta("b", "topic-2", "topic-3"),
			},
			Partitions: newPartitions(3, "topic-1", "topic-2", "topic-3"),
			Expected: memberGroupAssignments{
				"a": map[string][]int32{
					"topic-1": {0, 1, 2},
					"topic-2": {0, 2},
				},
				"b": map[string][]int32{
					"topic-2": {1},
					"topic-3": {0, 1, 2},
				},
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			assignments := roundrobinStrategy{}.AssignGroups(test.Members, test.Partitions)
			if !reflect.DeepEqual(test.Expected, assignments) {
				buf := bytes.NewBuffer(nil)
				encoder := json.NewEncoder(buf)
				encoder.SetIndent("", "  ")

				buf.WriteString("expected: ")
				encoder.Encode(test.Expected)
				buf.WriteString("got: ")
				encoder.Encode(assignments)

				t.Error(buf.String())
			}
		})
	}
}

func TestFindMembersByTopicSortsByMemberID(t *testing.T) {
	topic := "topic-1"
	a := memberGroupMetadata{
		MemberID: "a",
		Metadata: groupMetadata{
			Topics: []string{topic},
		},
	}
	b := memberGroupMetadata{
		MemberID: "b",
		Metadata: groupMetadata{
			Topics: []string{topic},
		},
	}
	c := memberGroupMetadata{
		MemberID: "c",
		Metadata: groupMetadata{
			Topics: []string{topic},
		},
	}

	testCases := map[string]struct {
		Data     []memberGroupMetadata
		Expected []memberGroupMetadata
	}{
		"in order": {
			Data:     []memberGroupMetadata{a, b},
			Expected: []memberGroupMetadata{a, b},
		},
		"out of order": {
			Data:     []memberGroupMetadata{a, c, b},
			Expected: []memberGroupMetadata{a, b, c},
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			membersByTopic := findMembersByTopic(test.Data)

			if actual := membersByTopic[topic]; !reflect.DeepEqual(test.Expected, actual) {
				t.Errorf("expected %v; got %v", test.Expected, actual)
			}
		})
	}
}
