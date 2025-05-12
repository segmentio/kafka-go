package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestFindMembersByTopic(t *testing.T) {
	a1 := GroupMember{
		ID:     "a",
		Topics: []string{"topic-1"},
	}
	a12 := GroupMember{
		ID:     "a",
		Topics: []string{"topic-1", "topic-2"},
	}
	b23 := GroupMember{
		ID:     "b",
		Topics: []string{"topic-2", "topic-3"},
	}

	tests := map[string]struct {
		Members  []GroupMember
		Expected map[string][]GroupMember
	}{
		"empty": {
			Expected: map[string][]GroupMember{},
		},
		"one member, one topic": {
			Members: []GroupMember{a1},
			Expected: map[string][]GroupMember{
				"topic-1": {
					a1,
				},
			},
		},
		"one member, multiple topics": {
			Members: []GroupMember{a12},
			Expected: map[string][]GroupMember{
				"topic-1": {
					a12,
				},
				"topic-2": {
					a12,
				},
			},
		},
		"multiple members, multiple topics": {
			Members: []GroupMember{a12, b23},
			Expected: map[string][]GroupMember{
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
	newMeta := func(memberID string, topics ...string) GroupMember {
		return GroupMember{
			ID:     memberID,
			Topics: topics,
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
		Members    []GroupMember
		Partitions []Partition
		Expected   GroupMemberAssignments
	}{
		"empty": {
			Expected: GroupMemberAssignments{},
		},
		"one member, one topic, one partition": {
			Members: []GroupMember{
				newMeta("a", "topic-1"),
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
				},
			},
		},
		"one member, one topic, multiple partitions": {
			Members: []GroupMember{
				newMeta("a", "topic-1"),
			},
			Partitions: newPartitions(3, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"multiple members, one topic, one partition": {
			Members: []GroupMember{
				newMeta("a", "topic-1"),
				newMeta("b", "topic-1"),
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{},
				"b": map[string][]int{
					"topic-1": {0},
				},
			},
		},
		"multiple members, one topic, multiple partitions": {
			Members: []GroupMember{
				newMeta("a", "topic-1"),
				newMeta("b", "topic-1"),
			},
			Partitions: newPartitions(3, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
				},
				"b": map[string][]int{
					"topic-1": {1, 2},
				},
			},
		},
		"multiple members, multiple topics, multiple partitions": {
			Members: []GroupMember{
				newMeta("a", "topic-1", "topic-2"),
				newMeta("b", "topic-2", "topic-3"),
			},
			Partitions: newPartitions(3, "topic-1", "topic-2", "topic-3"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0, 1, 2},
					"topic-2": {0},
				},
				"b": map[string][]int{
					"topic-2": {1, 2},
					"topic-3": {0, 1, 2},
				},
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			assignments := RangeGroupBalancer{}.AssignGroups(test.Members, test.Partitions)
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
	members := []GroupMember{}
	for i := 0; i < 66; i++ {
		members = append(members, GroupMember{
			ID:     strconv.Itoa(i),
			Topics: []string{"topic-1"},
		})
	}
	partitions := []Partition{}
	for i := 0; i < 213; i++ {
		partitions = append(partitions, Partition{
			ID:    i,
			Topic: "topic-1",
		})
	}

	assignments := RangeGroupBalancer{}.AssignGroups(members, partitions)
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
		Members    []GroupMember
		Partitions []Partition
		Expected   GroupMemberAssignments
	}{
		"empty": {
			Expected: GroupMemberAssignments{},
		},
		"one member, one topic, one partition": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic-1"},
				},
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
				},
			},
		},
		"one member, one topic, multiple partitions": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic-1"},
				},
			},
			Partitions: newPartitions(3, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0, 1, 2},
				},
			},
		},
		"multiple members, one topic, one partition": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic-1"},
				},
				{
					ID:     "b",
					Topics: []string{"topic-1"},
				},
			},
			Partitions: newPartitions(1, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
				},
				"b": map[string][]int{},
			},
		},
		"multiple members, multiple topics, multiple partitions": {
			Members: []GroupMember{
				{
					ID:     "a",
					Topics: []string{"topic-1", "topic-2"},
				},
				{
					ID:     "b",
					Topics: []string{"topic-2", "topic-3"},
				},
			},
			Partitions: newPartitions(3, "topic-1", "topic-2", "topic-3"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0, 1, 2},
					"topic-2": {0, 2},
				},
				"b": map[string][]int{
					"topic-2": {1},
					"topic-3": {0, 1, 2},
				},
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			assignments := RoundRobinGroupBalancer{}.AssignGroups(test.Members, test.Partitions)
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
	a := GroupMember{
		ID:     "a",
		Topics: []string{topic},
	}
	b := GroupMember{
		ID:     "b",
		Topics: []string{topic},
	}
	c := GroupMember{
		ID:     "c",
		Topics: []string{topic},
	}

	testCases := map[string]struct {
		Data     []GroupMember
		Expected []GroupMember
	}{
		"in order": {
			Data:     []GroupMember{a, b},
			Expected: []GroupMember{a, b},
		},
		"out of order": {
			Data:     []GroupMember{a, c, b},
			Expected: []GroupMember{a, b, c},
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

func TestRackAffinityGroupBalancer(t *testing.T) {
	t.Run("User Data", func(t *testing.T) {
		t.Run("unknown zone", func(t *testing.T) {
			b := RackAffinityGroupBalancer{}
			zone, err := b.UserData("", make(map[string][]int32), 0)
			if err != nil {
				t.Fatal(err)
			}
			if string(zone) != "" {
				t.Fatalf("expected empty zone but got %s", zone)
			}
		})

		t.Run("configure zone", func(t *testing.T) {
			b := RackAffinityGroupBalancer{Rack: "zone1"}
			zone, err := b.UserData("", make(map[string][]int32), 0)
			if err != nil {
				t.Fatal(err)
			}
			if string(zone) != "zone1" {
				t.Fatalf("expected zone1 az but got %s", zone)
			}
		})
	})

	t.Run("Balance", func(t *testing.T) {
		b := RackAffinityGroupBalancer{}

		brokers := map[string]Broker{
			"z1": {ID: 1, Rack: "z1"},
			"z2": {ID: 2, Rack: "z2"},
			"z3": {ID: 2, Rack: "z3"},
			"":   {},
		}

		tests := []struct {
			name            string
			memberCounts    map[string]int
			partitionCounts map[string]int
			result          map[string]map[string]int
		}{
			{
				name: "unknown and known zones",
				memberCounts: map[string]int{
					"":   1,
					"z1": 1,
					"z2": 1,
				},
				partitionCounts: map[string]int{
					"z1": 5,
					"z2": 4,
					"":   9,
				},
				result: map[string]map[string]int{
					"z1": {"": 1, "z1": 5},
					"z2": {"": 2, "z2": 4},
					"":   {"": 6},
				},
			},
			{
				name: "all unknown",
				memberCounts: map[string]int{
					"": 5,
				},
				partitionCounts: map[string]int{
					"": 103,
				},
				result: map[string]map[string]int{
					"": {"": 103},
				},
			},
			{
				name: "remainder stays local",
				memberCounts: map[string]int{
					"z1": 3,
					"z2": 3,
					"z3": 3,
				},
				partitionCounts: map[string]int{
					"z1": 20,
					"z2": 19,
					"z3": 20,
				},
				result: map[string]map[string]int{
					"z1": {"z1": 20},
					"z2": {"z2": 19},
					"z3": {"z3": 20},
				},
			},
			{
				name: "imbalanced partitions",
				memberCounts: map[string]int{
					"z1": 1,
					"z2": 1,
					"z3": 1,
				},
				partitionCounts: map[string]int{
					"z1": 7,
					"z2": 0,
					"z3": 7,
				},
				result: map[string]map[string]int{
					"z1": {"z1": 5},
					"z2": {"z1": 2, "z3": 2},
					"z3": {"z3": 5},
				},
			},
			{
				name: "imbalanced members",
				memberCounts: map[string]int{
					"z1": 5,
					"z2": 3,
					"z3": 1,
				},
				partitionCounts: map[string]int{
					"z1": 9,
					"z2": 9,
					"z3": 9,
				},
				result: map[string]map[string]int{
					"z1": {"z1": 9, "z3": 6},
					"z2": {"z2": 9},
					"z3": {"z3": 3},
				},
			},
			{
				name: "no consumers in zone",
				memberCounts: map[string]int{
					"z2": 10,
				},
				partitionCounts: map[string]int{
					"z1": 20,
					"z3": 19,
				},
				result: map[string]map[string]int{
					"z2": {"z1": 20, "z3": 19},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {

				// create members per the distribution in the test case.
				var members []GroupMember
				for zone, count := range tt.memberCounts {
					for i := 0; i < count; i++ {
						members = append(members, GroupMember{
							ID:       zone + ":" + strconv.Itoa(len(members)+1),
							Topics:   []string{"test"},
							UserData: []byte(zone),
						})
					}
				}

				// create partitions per the distribution in the test case.
				var partitions []Partition
				for zone, count := range tt.partitionCounts {
					for i := 0; i < count; i++ {
						partitions = append(partitions, Partition{
							ID:     len(partitions),
							Topic:  "test",
							Leader: brokers[zone],
						})
					}
				}

				res := b.AssignGroups(members, partitions)

				// verification #1...all members must be assigned and with the
				// correct load.
				minLoad := len(partitions) / len(members)
				maxLoad := minLoad
				if len(partitions)%len(members) != 0 {
					maxLoad++
				}
				for _, member := range members {
					assignments := res[member.ID]["test"]
					if len(assignments) < minLoad || len(assignments) > maxLoad {
						t.Errorf("expected between %d and %d partitions for member %s", minLoad, maxLoad, member.ID)
					}
				}

				// verification #2...all partitions are assigned, and the distribution
				// per source zone matches.
				partsPerZone := make(map[string]map[string]int)
				uniqueParts := make(map[int]struct{})
				for id, topicToPartitions := range res {

					for topic, assignments := range topicToPartitions {
						if topic != "test" {
							t.Fatalf("wrong topic...expected test but got %s", topic)
						}

						var member GroupMember
						for _, m := range members {
							if id == m.ID {
								member = m
								break
							}
						}
						if member.ID == "" {
							t.Fatal("empty member ID returned")
						}

						var partition Partition
						for _, id := range assignments {

							uniqueParts[id] = struct{}{}

							for _, p := range partitions {
								if p.ID == int(id) {
									partition = p
									break
								}
							}
							if partition.Topic == "" {
								t.Fatal("empty topic ID returned")
							}
							counts, ok := partsPerZone[string(member.UserData)]
							if !ok {
								counts = make(map[string]int)
								partsPerZone[string(member.UserData)] = counts
							}
							counts[partition.Leader.Rack]++
						}
					}
				}

				if len(partitions) != len(uniqueParts) {
					t.Error("not all partitions were assigned")
				}
				if !reflect.DeepEqual(tt.result, partsPerZone) {
					t.Errorf("wrong balanced zones.  expected %v but got %v", tt.result, partsPerZone)
				}
			})
		}
	})

	t.Run("Multi Topic", func(t *testing.T) {
		b := RackAffinityGroupBalancer{}

		brokers := map[string]Broker{
			"z1": {ID: 1, Rack: "z1"},
			"z2": {ID: 2, Rack: "z2"},
			"z3": {ID: 2, Rack: "z3"},
			"":   {},
		}

		members := []GroupMember{
			{
				ID:       "z1",
				Topics:   []string{"topic1", "topic2"},
				UserData: []byte("z1"),
			},
			{
				ID:       "z2",
				Topics:   []string{"topic2", "topic3"},
				UserData: []byte("z2"),
			},
			{
				ID:       "z3",
				Topics:   []string{"topic3", "topic1"},
				UserData: []byte("z3"),
			},
		}

		partitions := []Partition{
			{
				ID:     1,
				Topic:  "topic1",
				Leader: brokers["z1"],
			},
			{
				ID:     2,
				Topic:  "topic1",
				Leader: brokers["z3"],
			},
			{
				ID:     1,
				Topic:  "topic2",
				Leader: brokers["z1"],
			},
			{
				ID:     2,
				Topic:  "topic2",
				Leader: brokers["z2"],
			},
			{
				ID:     1,
				Topic:  "topic3",
				Leader: brokers["z3"],
			},
			{
				ID:     2,
				Topic:  "topic3",
				Leader: brokers["z2"],
			},
		}

		expected := GroupMemberAssignments{
			"z1": {"topic1": []int{1}, "topic2": []int{1}},
			"z2": {"topic2": []int{2}, "topic3": []int{2}},
			"z3": {"topic3": []int{1}, "topic1": []int{2}},
		}

		res := b.AssignGroups(members, partitions)
		if !reflect.DeepEqual(expected, res) {
			t.Fatalf("incorrect group assignment.  expected %v but got %v", expected, res)
		}
	})
}

func Test_deserializeTopicPartitionAssignment(t *testing.T) {
	type args struct {
		userDataBytes []byte
	}
	tests := []struct {
		name    string
		args    args
		want    StickyAssignorUserData
		wantErr bool
	}{
		{
			name: "Nil userdata bytes",
			args: args{},
			want: &StickyAssignorUserDataV0{},
		},
		{
			name: "Non-empty invalid userdata bytes",
			args: args{
				userDataBytes: []byte{
					0x00, 0x00,
					0x00, 0x00, 0x00, 0x01,
					0x00, 0x03, 'f', 'o', 'o',
				},
			},
			wantErr: true,
		},
		{
			name: "Valid v0 userdata bytes",
			args: args{
				userDataBytes: []byte{
					0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
					0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
					0xff,
				},
			},
			want: &StickyAssignorUserDataV0{
				Topics:     map[string][]int32{"t06": {0, 4}},
				Generation: -1,
				topicPartitions: []topicPartitionAssignment{
					{
						Topic:     "t06",
						Partition: 0,
					},
					{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := deserializeTopicPartitionAssignment(test.args.userDataBytes)
			if (err != nil) != test.wantErr {
				t.Errorf("deserializeTopicPartitionAssignment() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("deserializeTopicPartitionAssignment() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_prepopulateCurrentAssignments(t *testing.T) {
	type args struct {
		members []GroupMember
	}
	tests := []struct {
		name                   string
		args                   args
		wantCurrentAssignments map[string][]topicPartitionAssignment
		wantPrevAssignments    map[topicPartitionAssignment]consumerGenerationPair
		wantErr                bool
	}{
		{
			name:                   "Empty map",
			wantCurrentAssignments: map[string][]topicPartitionAssignment{},
			wantPrevAssignments:    map[topicPartitionAssignment]consumerGenerationPair{},
		},
		{
			name: "Single consumer",
			args: args{
				members: []GroupMember{
					{
						ID: "c01",
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
							0xff,
						},
					},
				},
			},
			wantCurrentAssignments: map[string][]topicPartitionAssignment{
				"c01": {
					{
						Topic:     "t06",
						Partition: 0,
					},
					{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
			wantPrevAssignments: map[topicPartitionAssignment]consumerGenerationPair{},
		},
		{
			name: "Duplicate consumer assignments in metadata",
			args: args{
				members: []GroupMember{
					{
						ID: "c01",
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
							0xff,
						},
					},
					{
						ID: "c02",
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
							0xff,
						},
					},
				},
			},
			wantCurrentAssignments: map[string][]topicPartitionAssignment{
				"c01": {
					{
						Topic:     "t06",
						Partition: 0,
					},
					{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
			wantPrevAssignments: map[topicPartitionAssignment]consumerGenerationPair{},
		},
		{
			name: "Different generations (5, 6) of consumer assignments in metadata",
			args: args{
				members: []GroupMember{
					{
						ID: "c01",
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
							0x05,
						},
					},
					{
						ID: "c02",
						UserData: []byte{
							0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x74, 0x30,
							0x36, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
							0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
							0x06,
						},
					},
				},
			},
			wantCurrentAssignments: map[string][]topicPartitionAssignment{
				"c01": {
					{
						Topic:     "t06",
						Partition: 0,
					},
					{
						Topic:     "t06",
						Partition: 4,
					},
				},
			},
			wantPrevAssignments: map[topicPartitionAssignment]consumerGenerationPair{
				{
					Topic:     "t06",
					Partition: 0,
				}: {
					Generation: 5,
					MemberID:   "c01",
				},
				{
					Topic:     "t06",
					Partition: 4,
				}: {
					Generation: 5,
					MemberID:   "c01",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, gotPrevAssignments, err := prepopulateCurrentAssignments(test.args.members)

			if (err != nil) != test.wantErr {
				t.Errorf("prepopulateCurrentAssignments() error = %v, wantErr %v", err, test.wantErr)
			}

			if !reflect.DeepEqual(gotPrevAssignments, test.wantPrevAssignments) {
				t.Errorf("deserializeTopicPartitionAssignment() prevAssignments = %v, here want %v", gotPrevAssignments, test.wantPrevAssignments)
			}
		})
	}
}

func Test_areSubscriptionsIdentical(t *testing.T) {
	type args struct {
		partition2AllPotentialConsumers map[topicPartitionAssignment][]string
		consumer2AllPotentialPartitions map[string][]topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty consumers and partitions",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: true,
		},
		{
			name: "Topic partitions with identical consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 2}: {"c1", "c2", "c3"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: true,
		},
		{
			name: "Topic partitions with mixed up consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c2", "c3", "c1"},
					{Topic: "t1", Partition: 2}: {"c3", "c1", "c2"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: true,
		},
		{
			name: "Topic partitions with different consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c2", "c3", "c1"},
					{Topic: "t1", Partition: 2}: {"cX", "c1", "c2"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: false,
		},
		{
			name: "Topic partitions with different number of consumer entries",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c2", "c3", "c1"},
					{Topic: "t1", Partition: 2}: {"c1", "c2"},
				},
				consumer2AllPotentialPartitions: make(map[string][]topicPartitionAssignment),
			},
			want: false,
		},
		{
			name: "Consumers with identical topic partitions",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c3": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
				},
			},
			want: true,
		},
		{
			name: "Consumer2 with mixed up consumer entries",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}, {Topic: "t1", Partition: 0}},
					"c3": {{Topic: "t1", Partition: 2}, {Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
				},
			},
			want: true,
		},
		{
			name: "Consumer2 with different consumer entries",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}, {Topic: "t1", Partition: 0}},
					"c3": {{Topic: "tX", Partition: 2}, {Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
				},
			},
			want: false,
		},
		{
			name: "Consumer2 with different number of consumer entries",
			args: args{
				partition2AllPotentialConsumers: make(map[topicPartitionAssignment][]string),
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}, {Topic: "t1", Partition: 0}},
					"c3": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
				},
			},
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := areSubscriptionsIdentical(test.args.partition2AllPotentialConsumers, test.args.consumer2AllPotentialPartitions); got != test.want {
				t.Errorf("areSubscriptionsIdentical() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_sortMemberIDsByPartitionAssignments(t *testing.T) {
	type args struct {
		assignments map[string][]topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Null assignments",
			want: make([]string, 0),
		},
		{
			name: "Single assignment",
			args: args{
				assignments: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
						{Topic: "t1", Partition: 2},
					},
				},
			},
			want: []string{"c1"},
		},
		{
			name: "Multiple assignments with different partition counts",
			args: args{
				assignments: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
					},
					"c2": {
						{Topic: "t1", Partition: 1},
						{Topic: "t1", Partition: 2},
					},
					"c3": {
						{Topic: "t1", Partition: 3},
						{Topic: "t1", Partition: 4},
						{Topic: "t1", Partition: 5},
					},
				},
			},
			want: []string{"c1", "c2", "c3"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sortMemberIDsByPartitionAssignments(test.args.assignments); !reflect.DeepEqual(got, test.want) {
				t.Errorf("sortMemberIDsByPartitionAssignments() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_sortPartitions(t *testing.T) {
	type args struct {
		currentAssignment                          map[string][]topicPartitionAssignment
		partitionsWithADifferentPreviousAssignment map[topicPartitionAssignment]consumerGenerationPair
		isFreshAssignment                          bool
		partition2AllPotentialConsumers            map[topicPartitionAssignment][]string
		consumer2AllPotentialPartitions            map[string][]topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want []topicPartitionAssignment
	}{
		{
			name: "Empty everything",
			want: make([]topicPartitionAssignment, 0),
		},
		{
			name: "Base case",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}},
					"c2": {{Topic: "t1", Partition: 1}},
					"c3": {{Topic: "t1", Partition: 2}},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c3": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c2", "c3", "c1"},
					{Topic: "t1", Partition: 2}: {"c3", "c1", "c2"},
				},
			},
		},
		{
			name: "Partitions assigned to a different consumer last time",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c3": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c2", "c3", "c1"},
					{Topic: "t1", Partition: 2}: {"c3", "c1", "c2"},
				},
				partitionsWithADifferentPreviousAssignment: map[topicPartitionAssignment]consumerGenerationPair{
					{Topic: "t1", Partition: 0}: {Generation: 1, MemberID: "c2"},
				},
			},
		},
		{
			name: "Partitions assigned to a different consumer last time",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}},
					"c2": {{Topic: "t1", Partition: 1}},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c3": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c2", "c3", "c1"},
					{Topic: "t1", Partition: 2}: {"c3", "c1", "c2"},
				},
				partitionsWithADifferentPreviousAssignment: map[topicPartitionAssignment]consumerGenerationPair{
					{Topic: "t1", Partition: 0}: {Generation: 1, MemberID: "c2"},
				},
			},
		},
		{
			name: "Fresh assignment",
			args: args{
				isFreshAssignment: true,
				currentAssignment: map[string][]topicPartitionAssignment{},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c2": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
					"c3": {{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2", "c3"},
					{Topic: "t1", Partition: 1}: {"c2", "c3", "c1"},
					{Topic: "t1", Partition: 2}: {"c3", "c1", "c2"},
				},
				partitionsWithADifferentPreviousAssignment: map[topicPartitionAssignment]consumerGenerationPair{
					{Topic: "t1", Partition: 0}: {Generation: 1, MemberID: "c2"},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := sortPartitions(test.args.currentAssignment, test.args.partitionsWithADifferentPreviousAssignment, test.args.isFreshAssignment, test.args.partition2AllPotentialConsumers, test.args.consumer2AllPotentialPartitions)
			if test.want != nil && !reflect.DeepEqual(got, test.want) {
				t.Errorf("sortPartitions() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_filterAssignedPartitions(t *testing.T) {
	type args struct {
		currentAssignment               map[string][]topicPartitionAssignment
		partition2AllPotentialConsumers map[topicPartitionAssignment][]string
	}
	tests := []struct {
		name string
		args args
		want map[string][]topicPartitionAssignment
	}{
		{
			name: "All partitions accounted for",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}},
					"c2": {{Topic: "t1", Partition: 1}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1"},
					{Topic: "t1", Partition: 1}: {"c2"},
				},
			},
			want: map[string][]topicPartitionAssignment{
				"c1": {{Topic: "t1", Partition: 0}},
				"c2": {{Topic: "t1", Partition: 1}},
			},
		},
		{
			name: "One consumer using an unrecognized partition",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}},
					"c2": {{Topic: "t1", Partition: 1}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1"},
				},
			},
			want: map[string][]topicPartitionAssignment{
				"c1": {{Topic: "t1", Partition: 0}},
				"c2": {},
			},
		},
		{
			name: "Interleaved consumer removal",
			args: args{
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {{Topic: "t1", Partition: 0}},
					"c2": {{Topic: "t1", Partition: 1}},
					"c3": {{Topic: "t1", Partition: 2}},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1"},
					{Topic: "t1", Partition: 2}: {"c3"},
				},
			},
			want: map[string][]topicPartitionAssignment{
				"c1": {{Topic: "t1", Partition: 0}},
				"c2": {},
				"c3": {{Topic: "t1", Partition: 2}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := filterAssignedPartitions(test.args.currentAssignment, test.args.partition2AllPotentialConsumers); !reflect.DeepEqual(got, test.want) {
				t.Errorf("filterAssignedPartitions() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_canConsumerParticipateInReassignment(t *testing.T) {
	type args struct {
		memberID                        string
		currentAssignment               map[string][]topicPartitionAssignment
		consumer2AllPotentialPartitions map[string][]topicPartitionAssignment
		partition2AllPotentialConsumers map[topicPartitionAssignment][]string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Consumer has been assigned partitions not available to it",
			args: args{
				memberID: "c1",
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
						{Topic: "t1", Partition: 2},
					},
					"c2": {},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
					},
					"c2": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
						{Topic: "t1", Partition: 2},
					},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1", "c2"},
					{Topic: "t1", Partition: 1}: {"c1", "c2"},
					{Topic: "t1", Partition: 2}: {"c2"},
				},
			},
			want: true,
		},
		{
			name: "Consumer has been assigned all available partitions",
			args: args{
				memberID: "c1",
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
					},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
					},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1"},
					{Topic: "t1", Partition: 1}: {"c1"},
				},
			},
			want: false,
		},
		{
			name: "Consumer has not been assigned all available partitions",
			args: args{
				memberID: "c1",
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
					},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
						{Topic: "t1", Partition: 1},
						{Topic: "t1", Partition: 2},
					},
				},
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{Topic: "t1", Partition: 0}: {"c1"},
					{Topic: "t1", Partition: 1}: {"c1"},
					{Topic: "t1", Partition: 2}: {"c1"},
				},
			},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := canConsumerParticipateInReassignment(test.args.memberID, test.args.currentAssignment, test.args.consumer2AllPotentialPartitions, test.args.partition2AllPotentialConsumers); got != test.want {
				t.Errorf("canConsumerParticipateInReassignment() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_removeTopicPartitionFromMemberAssignments(t *testing.T) {
	type args struct {
		assignments []topicPartitionAssignment
		topic       topicPartitionAssignment
	}
	tests := []struct {
		name string
		args args
		want []topicPartitionAssignment
	}{
		{
			name: "Empty",
			args: args{
				assignments: make([]topicPartitionAssignment, 0),
				topic:       topicPartitionAssignment{Topic: "t1", Partition: 0},
			},
			want: make([]topicPartitionAssignment, 0),
		},
		{
			name: "Remove first entry",
			args: args{
				assignments: []topicPartitionAssignment{
					{Topic: "t1", Partition: 0},
					{Topic: "t1", Partition: 1},
					{Topic: "t1", Partition: 2},
				},
				topic: topicPartitionAssignment{Topic: "t1", Partition: 0},
			},
			want: []topicPartitionAssignment{
				{Topic: "t1", Partition: 1},
				{Topic: "t1", Partition: 2},
			},
		},
		{
			name: "Remove middle entry",
			args: args{
				assignments: []topicPartitionAssignment{
					{Topic: "t1", Partition: 0},
					{Topic: "t1", Partition: 1},
					{Topic: "t1", Partition: 2},
				},
				topic: topicPartitionAssignment{Topic: "t1", Partition: 1},
			},
			want: []topicPartitionAssignment{
				{Topic: "t1", Partition: 0},
				{Topic: "t1", Partition: 2},
			},
		},
		{
			name: "Remove last entry",
			args: args{
				assignments: []topicPartitionAssignment{
					{Topic: "t1", Partition: 0},
					{Topic: "t1", Partition: 1},
					{Topic: "t1", Partition: 2},
				},
				topic: topicPartitionAssignment{Topic: "t1", Partition: 2},
			},
			want: []topicPartitionAssignment{
				{Topic: "t1", Partition: 0},
				{Topic: "t1", Partition: 1},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := removeTopicPartitionFromMemberAssignments(test.args.assignments, test.args.topic); !reflect.DeepEqual(got, test.want) {
				t.Errorf("removeTopicPartitionFromMemberAssignments() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_assignPartition(t *testing.T) {
	type args struct {
		partition                       topicPartitionAssignment
		sortedCurrentSubscriptions      []string
		currentAssignment               map[string][]topicPartitionAssignment
		consumer2AllPotentialPartitions map[string][]topicPartitionAssignment
		currentPartitionConsumer        map[topicPartitionAssignment]string
	}
	tests := []struct {
		name                         string
		args                         args
		want                         []string
		wantCurrentAssignment        map[string][]topicPartitionAssignment
		wantCurrentPartitionConsumer map[topicPartitionAssignment]string
	}{
		{
			name: "Base",
			args: args{
				partition:                  topicPartitionAssignment{Topic: "t1", Partition: 2},
				sortedCurrentSubscriptions: []string{"c3", "c1", "c2"},
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
					},
					"c2": {
						{Topic: "t1", Partition: 1},
					},
					"c3": {},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
					},
					"c2": {
						{Topic: "t1", Partition: 1},
					},
					"c3": {
						{Topic: "t1", Partition: 2},
					},
				},
				currentPartitionConsumer: map[topicPartitionAssignment]string{
					{Topic: "t1", Partition: 0}: "c1",
					{Topic: "t1", Partition: 1}: "c2",
				},
			},
			want: []string{"c1", "c2", "c3"},
			wantCurrentAssignment: map[string][]topicPartitionAssignment{
				"c1": {
					{Topic: "t1", Partition: 0},
				},
				"c2": {
					{Topic: "t1", Partition: 1},
				},
				"c3": {
					{Topic: "t1", Partition: 2},
				},
			},
			wantCurrentPartitionConsumer: map[topicPartitionAssignment]string{
				{Topic: "t1", Partition: 0}: "c1",
				{Topic: "t1", Partition: 1}: "c2",
				{Topic: "t1", Partition: 2}: "c3",
			},
		},
		{
			name: "Unassignable Partition",
			args: args{
				partition:                  topicPartitionAssignment{Topic: "t1", Partition: 3},
				sortedCurrentSubscriptions: []string{"c3", "c1", "c2"},
				currentAssignment: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
					},
					"c2": {
						{Topic: "t1", Partition: 1},
					},
					"c3": {},
				},
				consumer2AllPotentialPartitions: map[string][]topicPartitionAssignment{
					"c1": {
						{Topic: "t1", Partition: 0},
					},
					"c2": {
						{Topic: "t1", Partition: 1},
					},
					"c3": {
						{Topic: "t1", Partition: 2},
					},
				},
				currentPartitionConsumer: map[topicPartitionAssignment]string{
					{Topic: "t1", Partition: 0}: "c1",
					{Topic: "t1", Partition: 1}: "c2",
				},
			},
			want: []string{"c3", "c1", "c2"},
			wantCurrentAssignment: map[string][]topicPartitionAssignment{
				"c1": {
					{Topic: "t1", Partition: 0},
				},
				"c2": {
					{Topic: "t1", Partition: 1},
				},
				"c3": {},
			},
			wantCurrentPartitionConsumer: map[topicPartitionAssignment]string{
				{Topic: "t1", Partition: 0}: "c1",
				{Topic: "t1", Partition: 1}: "c2",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := assignPartition(test.args.partition, test.args.sortedCurrentSubscriptions, test.args.currentAssignment, test.args.consumer2AllPotentialPartitions, test.args.currentPartitionConsumer); !reflect.DeepEqual(got, test.want) {
				t.Errorf("assignPartition() = %v, want %v", got, test.want)
			}
			if !reflect.DeepEqual(test.args.currentAssignment, test.wantCurrentAssignment) {
				t.Errorf("assignPartition() currentAssignment = %v, want %v", test.args.currentAssignment, test.wantCurrentAssignment)
			}
			if !reflect.DeepEqual(test.args.currentPartitionConsumer, test.wantCurrentPartitionConsumer) {
				t.Errorf("assignPartition() currentPartitionConsumer = %v, want %v", test.args.currentPartitionConsumer, test.wantCurrentPartitionConsumer)
			}
		})
	}
}

func Test_stickyBalanceStrategy_Plan(t *testing.T) {
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

	type args struct {
		members []GroupMember
		topics  []Partition
	}
	tests := []struct {
		name string
		s    *StickyGroupBalancer
		args args
	}{
		{
			name: "One consumer with no topics",
			args: args{
				members: []GroupMember{
					{ID: "consumer"},
				},
				topics: newPartitions(0, ""),
			},
		},
		{
			name: "One consumer with non-existent topic",
			args: args{
				members: []GroupMember{
					{
						ID:     "consumer",
						Topics: []string{"topic"},
					},
				},
				topics: newPartitions(0, "topic"),
			},
		},
		{
			name: "One consumer with one topic",
			args: args{
				members: []GroupMember{
					{
						ID:     "consumer",
						Topics: []string{"topic"},
					},
				},
				topics: newPartitions(3, "topic"),
			},
		},
		{
			name: "Only assigns partitions from subscribed topics",
			args: args{
				members: []GroupMember{
					{
						ID:     "consumer",
						Topics: []string{"topic"},
					},
				},
				topics: newPartitions(3, "topic", "other"),
			},
		},
		{
			name: "One consumer with multiple topics",
			args: args{
				members: []GroupMember{
					{ID: "consumer",
						Topics: []string{"topic1", "topic2"},
					},
				},
				topics: []Partition{
					{
						Topic: "topic1",
						ID:    0,
					},
					{
						Topic: "topic2",
						ID:    0,
					},
					{
						Topic: "topic2",
						ID:    1,
					},
				},
			},
		},
		{
			name: "Two consumers with one topic and one partition",
			args: args{
				members: []GroupMember{
					{
						ID:     "consumer1",
						Topics: []string{"topic"},
					},
					{
						ID:     "consumer2",
						Topics: []string{"topic"},
					},
				},
				topics: []Partition{
					{
						Topic: "topic",
						ID:    0,
					},
				},
			},
		},
		{
			name: "Two consumers with one topic and two partitions",
			args: args{
				members: []GroupMember{
					{
						ID:     "consumer1",
						Topics: []string{"topic"},
					},
					{
						ID:     "consumer2",
						Topics: []string{"topic"},
					},
				},
				topics: []Partition{
					{
						Topic: "topic",
						ID:    0,
					},
					{
						Topic: "topic",
						ID:    1,
					},
				},
			},
		},
		{
			name: "Multiple consumers with mixed topic subscriptions",
			args: args{
				members: []GroupMember{
					{
						ID:     "consumer1",
						Topics: []string{"topic1"},
					},
					{
						ID:     "consumer2",
						Topics: []string{"topic1", "topic2"},
					},
					{
						ID:     "consumer3",
						Topics: []string{"topic1"},
					},
				},
				topics: []Partition{
					{
						Topic: "topic1",
						ID:    0,
					},
					{
						Topic: "topic1",
						ID:    1,
					},
					{
						Topic: "topic1",
						ID:    2,
					},
					{
						Topic: "topic2",
						ID:    0,
					},
					{
						Topic: "topic2",
						ID:    1,
					},
				},
			},
		},
		{
			name: "Two consumers with two topics and six partitions",
			args: args{
				members: []GroupMember{
					{
						ID:     "consumer1",
						Topics: []string{"topic1", "topic2"},
					},
					{
						ID:     "consumer2",
						Topics: []string{"topic1", "topic2"},
					},
				},
				topics: newPartitions(3, "topic1", "topic2"),
			},
		},
		{
			name: "Three consumers (two old, one new) with one topic and twelve partitions",
			args: args{
				members: []GroupMember{
					{
						ID:       "consumer1",
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {4, 11, 8, 5, 9, 2}}, 1),
					},
					{
						ID:       "consumer2",
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {1, 3, 0, 7, 10, 6}}, 1),
					},
					{
						ID:     "consumer3",
						Topics: []string{"topic1"},
					},
				},
				topics: newPartitions(12, "topic1"),
			},
		},
		{
			name: "Three consumers (two old, one new) with one topic and 13 partitions",
			args: args{
				members: []GroupMember{
					{
						ID:       "consumer1",
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {4, 11, 8, 5, 9, 2, 6}}, 1),
					},
					{
						ID:       "consumer2",
						Topics:   []string{"topic1"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {1, 3, 0, 7, 10, 12}}, 1),
					},
					{
						ID:     "consumer3",
						Topics: []string{"topic1"},
					},
				},
				topics: newPartitions(13, "topic1"),
			},
		},
		{
			name: "One consumer that is no longer subscribed to a topic that it had previously been consuming from",
			args: args{
				members: []GroupMember{
					{
						ID:       "consumer1",
						Topics:   []string{"topic2"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {0}}, 1),
					},
				},
				topics: newPartitions(1, "topic1", "topic2"),
			},
		},
		{
			name: "Two consumers where one is no longer interested in consuming from a topic that it had been consuming from",
			args: args{
				members: []GroupMember{
					{
						ID:       "consumer1",
						Topics:   []string{"topic2"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {0}}, 1),
					},
					{
						ID:       "consumer2",
						Topics:   []string{"topic1", "topic2"},
						UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {1}}, 1),
					},
				},
				topics: newPartitions(2, "topic1", "topic2"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &StickyGroupBalancer{}

			plan := s.AssignGroups(test.args.members, test.args.topics)
			verifyPlanIsBalancedAndSticky(t, s, test.args.members, plan)
			verifyFullyBalanced(t, plan)
		})
	}
}

func verifyPlanIsBalancedAndSticky(t *testing.T, s *StickyGroupBalancer, members []GroupMember, plan GroupMemberAssignments) {
	t.Helper()
	if !s.movements.isSticky() {
		t.Error("stickyBalanceStrategy.Plan() not sticky")
		return
	}
	verifyValidityAndBalance(t, members, plan)
}
func verifyValidityAndBalance(t *testing.T, consumers []GroupMember, plan GroupMemberAssignments) {
	t.Helper()
	size := len(consumers)
	if size != len(plan) {
		t.Errorf("Subscription size (%d) not equal to plan size (%d), consumers: %v \t plan:%v ", size, len(plan), consumers, plan)
		t.FailNow()
	}

	members := make([]string, size)
	i := 0
	for _, member := range consumers {
		members[i] = member.ID
		i++
	}
	sort.Strings(members)
	//map of consumers by ID
	membersByID := make(map[string]GroupMember)
	for _, consumer := range consumers {
		membersByID[consumer.ID] = consumer
	}
	for i, memberID := range members {
		for assignedTopic := range plan[memberID] {
			found := false
			for _, assignableTopic := range membersByID[memberID].Topics {
				if assignableTopic == assignedTopic {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Consumer %s had assigned topic %q that wasn't in the list of assignable topics %v", memberID, assignedTopic, membersByID[memberID].Topics)
				t.FailNow()
			}
		}

		// skip last consumer
		if i == len(members)-1 {
			continue
		}

		consumerAssignments := make([]topicPartitionAssignment, 0)
		for topic, partitions := range plan[memberID] {
			for _, partition := range partitions {
				consumerAssignments = append(consumerAssignments, topicPartitionAssignment{Topic: topic, Partition: int32(partition)})
			}
		}

		for j := i + 1; j < size; j++ {
			otherConsumer := members[j]
			otherConsumerAssignments := make([]topicPartitionAssignment, 0)
			for topic, partitions := range plan[otherConsumer] {
				for _, partition := range partitions {
					otherConsumerAssignments = append(otherConsumerAssignments, topicPartitionAssignment{Topic: topic, Partition: int32(partition)})
				}
			}
			assignmentsIntersection := intersection(consumerAssignments, otherConsumerAssignments)
			if len(assignmentsIntersection) > 0 {
				t.Errorf("Consumers %s and %s have common partitions assigned to them: %v", memberID, otherConsumer, assignmentsIntersection)
				t.FailNow()
			}

			if math.Abs(float64(len(consumerAssignments)-len(otherConsumerAssignments))) <= 1 {
				continue
			}

			if len(consumerAssignments) > len(otherConsumerAssignments) {
				for _, topic := range consumerAssignments {
					if _, exists := plan[otherConsumer][topic.Topic]; exists {
						t.Errorf("Some partitions can be moved from %s to %s to achieve a better balance, %s has %d assignments, and %s has %d assignments", otherConsumer, memberID, memberID, len(consumerAssignments), otherConsumer, len(otherConsumerAssignments))
						t.FailNow()
					}
				}
			}

			if len(otherConsumerAssignments) > len(consumerAssignments) {
				for _, topic := range otherConsumerAssignments {
					if _, exists := plan[memberID][topic.Topic]; exists {
						t.Errorf("Some partitions can be moved from %s to %s to achieve a better balance, %s has %d assignments, and %s has %d assignments", memberID, otherConsumer, otherConsumer, len(otherConsumerAssignments), memberID, len(consumerAssignments))
						t.FailNow()
					}
				}
			}
		}
	}
}

// Produces the intersection of two slices
// From https://github.com/juliangruber/go-intersect
func intersection(a interface{}, b interface{}) []interface{} {
	set := make([]interface{}, 0)
	hash := make(map[interface{}]bool)
	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)

	for i := 0; i < av.Len(); i++ {
		el := av.Index(i).Interface()
		hash[el] = true
	}

	for i := 0; i < bv.Len(); i++ {
		el := bv.Index(i).Interface()
		if _, found := hash[el]; found {
			set = append(set, el)
		}
	}

	return set
}

func encodeSubscriberPlan(t *testing.T, assignments map[string][]int32) []byte {
	return encodeSubscriberPlanWithGeneration(t, assignments, defaultGeneration)
}

func encodeSubscriberPlanWithGeneration(t *testing.T, assignments map[string][]int32, generation int32) []byte {
	userDataBytes := (&StickyAssignorUserDataV0{
		Topics: assignments,
	}).bytes()
	return userDataBytes
}

// verify that the plan is fully balanced, assumes that all consumers can
// consume from the same set of topics.
func verifyFullyBalanced(t *testing.T, plan GroupMemberAssignments) {
	min := math.MaxInt32
	max := math.MinInt32
	for _, topics := range plan {
		assignedPartitionsCount := 0
		for _, partitions := range topics {
			assignedPartitionsCount += len(partitions)
		}
		if assignedPartitionsCount < min {
			min = assignedPartitionsCount
		}
		if assignedPartitionsCount > max {
			max = assignedPartitionsCount
		}
	}
	if (max - min) > 1 {
		t.Errorf("Plan partition assignment is not fully balanced: min=%d, max=%d", min, max)
	}
}

func Test_stickyBalanceStrategy_Plan_KIP54_ExampleOne(t *testing.T) {
	s := &StickyGroupBalancer{}
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

	// PLAN 1
	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic1", "topic2", "topic3", "topic4"},
		},
		{
			ID:     "consumer2",
			Topics: []string{"topic1", "topic2", "topic3", "topic4"},
		},
		{
			ID:     "consumer3",
			Topics: []string{"topic1", "topic2", "topic3", "topic4"},
		},
	}
	topics := newPartitions(2, "topic1", "topic2", "topic3")
	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)
	verifyFullyBalanced(t, plan1)

	membersByID := make(map[string]GroupMember)
	for _, consumer := range members {
		membersByID[consumer.ID] = consumer
	}

	// PLAN 2
	var indexofmembertobedeleted int
	for ind, member := range members {
		if member.ID == "consumer1" {
			indexofmembertobedeleted = ind
		}
		topics32 := make(map[string][]int32)
		ttopics := plan1[member.ID]
		for topic, partitions := range ttopics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)

	}

	members = append(members[:indexofmembertobedeleted], members[indexofmembertobedeleted+1:]...)
	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
	verifyFullyBalanced(t, plan2)
}

func Test_stickyBalanceStrategy_Plan_KIP54_ExampleTwo(t *testing.T) {
	s := &StickyGroupBalancer{}

	// PLAN 1
	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic1"},
		},
		{
			ID:     "consumer2",
			Topics: []string{"topic1", "topic2"},
		},
		{
			ID:     "consumer3",
			Topics: []string{"topic1", "topic2", "topic3"},
		},
	}

	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
		{
			Topic: "topic2",
			ID:    0,
		},
		{
			Topic: "topic2",
			ID:    1,
		},
		{
			Topic: "topic3",
			ID:    0,
		},
		{
			Topic: "topic3",
			ID:    1,
		},
		{
			Topic: "topic3",
			ID:    2,
		},
	}

	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)
	if len(plan1["consumer1"]["topic1"]) != 1 || len(plan1["consumer2"]["topic2"]) != 2 || len(plan1["consumer3"]["topic3"]) != 3 {
		t.Error("Incorrect distribution of topic partition assignments")
	}

	// PLAN 2
	var indexofmembertobedeleted int
	for ind, member := range members {
		if member.ID == "consumer1" {
			indexofmembertobedeleted = ind
		}
		topics32 := make(map[string][]int32)
		ttopics := plan1[member.ID]
		for topic, partitions := range ttopics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)

	}
	members = append(members[:indexofmembertobedeleted], members[indexofmembertobedeleted+1:]...)

	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
	verifyFullyBalanced(t, plan2)
	if len(plan2["consumer2"]["topic1"]) != 1 || len(plan2["consumer2"]["topic2"]) != 2 || len(plan2["consumer3"]["topic3"]) != 3 {
		t.Error("Incorrect distribution of topic partition assignments")
	}
}

func Test_stickyBalanceStrategy_Plan_KIP54_ExampleThree(t *testing.T) {
	s := &StickyGroupBalancer{}
	topicNames := []string{"topic1", "topic2"}

	// PLAN 1
	members := []GroupMember{
		{ID: "consumer1",
			Topics: topicNames,
		},
		{
			ID:     "consumer2",
			Topics: topicNames,
		},
	}
	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
		{
			Topic: "topic1",
			ID:    1,
		},
		{
			Topic: "topic2",
			ID:    0,
		},
		{
			Topic: "topic2",
			ID:    1,
		},
	}
	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	// PLAN 2
	var consumer3 GroupMember
	for _, member := range members {
		if member.ID == "consumer1" {
			continue
		}
		topics32 := make(map[string][]int32)
		ttopics := plan1[member.ID]
		for topic, partitions := range ttopics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)
		if member.ID == "consumer3" {
			consumer3 = member
		}
	}
	if consumer3.ID != "" {
		topics32 := make(map[string][]int32)
		ttopics := plan1["consumer3"]
		for topic, partitions := range ttopics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		consumer3 = GroupMember{ID: "consumer3",
			Topics:   topicNames,
			UserData: encodeSubscriberPlan(t, topics32),
		}
		members = append(members, consumer3)
	}

	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
	verifyFullyBalanced(t, plan2)
}

func Test_stickyBalanceStrategy_Plan_AddRemoveConsumerOneTopic(t *testing.T) {
	s := &StickyGroupBalancer{}

	// PLAN 1
	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic"},
		},
	}
	topics := []Partition{
		{
			Topic: "topic",
			ID:    0,
		},
		{
			Topic: "topic",
			ID:    1,
		},
		{
			Topic: "topic",
			ID:    2,
		},
	}
	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	// PLAN 2
	var consumer2 GroupMember
	for _, member := range members {
		topics32 := make(map[string][]int32)
		topics := plan1[member.ID]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)
		if member.ID == "consumer2" {
			consumer2 = member
		}
	}
	if consumer2.ID != "" {
		topics32 := make(map[string][]int32)
		topics := plan1["consumer2"]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		consumer2 = GroupMember{ID: "consumer2",
			Topics:   []string{"topic"},
			UserData: encodeSubscriberPlan(t, topics32),
		}
		members = append(members, consumer2)
	}

	plan2 := s.AssignGroups(members, topics)

	verifyPlanIsBalancedAndSticky(t, s, members, plan2)

	// PLAN 3
	var consumer3 GroupMember
	var indexofmembertobedeleted int
	for ind, member := range members {
		if member.ID == "consumer1" {
			indexofmembertobedeleted = ind
			continue
		}
		topics32 := make(map[string][]int32)
		topics := plan1[member.ID]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)
		if member.ID == "consumer2" {
			consumer3 = member
		}

	}
	if consumer3.ID != "" {
		topics32 := make(map[string][]int32)
		topics := plan1["consumer2"]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		consumer3 = GroupMember{ID: "consumer2",
			Topics:   []string{"topic"},
			UserData: encodeSubscriberPlan(t, topics32),
		}
		members = append(members, consumer3)
	}
	members = append(members[:indexofmembertobedeleted], members[indexofmembertobedeleted+1:]...)
	plan3 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan3)
}

func Test_stickyBalanceStrategy_Plan_PoorRoundRobinAssignmentScenario(t *testing.T) {
	s := &StickyGroupBalancer{}

	// PLAN 1
	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5"},
		},
		{
			ID:     "consumer2",
			Topics: []string{"topic1", "topic3", "topic5"},
		},
		{
			ID:     "consumer3",
			Topics: []string{"topic1", "topic3", "topic5"},
		},
		{
			ID:     "consumer4",
			Topics: []string{"topic1", "topic2", "topic3", "topic4", "topic5"},
		},
	}
	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
		{
			Topic: "topic1",
			ID:    1,
		},
		{
			Topic: "topic2",
			ID:    0,
		},
		{
			Topic: "topic3",
			ID:    0,
		},
		{
			Topic: "topic3",
			ID:    1,
		},
		{
			Topic: "topic4",
			ID:    0,
		},
		{
			Topic: "topic5",
			ID:    0,
		},
		{
			Topic: "topic5",
			ID:    1,
		},
	}
	plan := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan)
}

func Test_stickyBalanceStrategy_Plan_AddRemoveTopicTwoConsumers(t *testing.T) {
	s := &StickyGroupBalancer{}

	// PLAN 1
	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic1"},
		},
		{
			ID:     "consumer2",
			Topics: []string{"topic1"},
		},
	}
	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
		{
			Topic: "topic1",
			ID:    1,
		},
		{
			Topic: "topic1",
			ID:    2,
		},
	}
	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)
	verifyFullyBalanced(t, plan1)

	// PLAN 2
	for _, member := range members {
		topics32 := make(map[string][]int32)
		topics := plan1[member.ID]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.Topics = []string{"topic1", "topic2"}
	}
	topic2 := []Partition{{},
		{Topic: "topic2"},
		{
			Topic: "topic2",
			ID:    0,
		},
		{
			Topic: "topic2",
			ID:    1,
		},
		{
			Topic: "topic2",
			ID:    2,
		},
	}
	topics = append(topics, topic2...)

	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
	verifyFullyBalanced(t, plan2)

	// PLAN 3
	for _, member := range members {
		topics32 := make(map[string][]int32)
		topics := plan1[member.ID]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)
	}
	indsoftopicstobedeleted := make([]int, 0)
	for ind, partition := range topics {
		if partition.Topic == "topic1" {
			indsoftopicstobedeleted = append(indsoftopicstobedeleted, ind)
		}
	}

	for _, ind := range indsoftopicstobedeleted {
		topics = append(topics[:ind], topics[ind+1:]...)
	}
	plan3 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan3)
	verifyFullyBalanced(t, plan3)
}

func Test_stickyBalanceStrategy_Plan_ReassignmentAfterOneConsumerAdded(t *testing.T) {
	s := &StickyGroupBalancer{}

	// PLAN 1
	members := []GroupMember{}
	for i := 0; i < 10; i++ {
		members = append(members, GroupMember{ID: fmt.Sprintf("consumer%d", i),
			Topics: []string{"topic1"}})
	}
	topics := []Partition{}
	for j := 0; j < 20; j++ {
		topics = append(topics, Partition{Topic: "topic1", ID: j})
	}

	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	// add a new consumer
	members = append(members, GroupMember{ID: "consumer10", Topics: []string{"topic1"}})
	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
}

func Test_stickyBalanceStrategy_Plan_SameSubscriptions(t *testing.T) {
	s := &StickyGroupBalancer{}

	// PLAN 1
	members := []GroupMember{}
	for i := 0; i < 9; i++ {
		topics := make([]string, 15)
		for j := 0; j < 15; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members = append(members, GroupMember{ID: fmt.Sprintf("consumer%d", i), Topics: topics})
	}
	topics := []Partition{}
	for i := 0; i < 15; i++ {
		topic := fmt.Sprintf("topic%d", i)
		for j := 0; j < i; j++ {
			topics = append(topics, Partition{Topic: topic, ID: j})
		}
	}

	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	// PLAN 2
	var indexofmembertobedeleted int
	for ind, member := range members {
		if member.ID == "consumer5" {
			indexofmembertobedeleted = ind
		}
		topics32 := make(map[string][]int32)
		topics := plan1[member.ID]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)
	}

	members = append(members[:indexofmembertobedeleted], members[indexofmembertobedeleted+1:]...)

	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
}

func Test_stickyBalanceStrategy_Plan_LargeAssignmentWithMultipleConsumersLeaving(t *testing.T) {
	s := &StickyGroupBalancer{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// PLAN 1
	members := []GroupMember{}
	for i := 0; i < 200; i++ {
		topics := make([]string, 200)
		for j := 0; j < 200; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members = append(members, GroupMember{ID: fmt.Sprintf("consumer%d", i), Topics: topics})
	}
	topics := []Partition{}
	for i := 0; i < 40; i++ {
		partitionCount := r.Intn(20)
		topic := fmt.Sprintf("topic%d", i)
		for j := 0; j < partitionCount; j++ {
			topics = append(topics, Partition{ID: j, Topic: topic})
		}
	}

	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	for _, member := range members {
		topics32 := make(map[string][]int32)
		ttopics := plan1[member.ID]
		for topic, partitions := range ttopics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlan(t, topics32)
	}
	members = members[50:]

	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
}

func Test_stickyBalanceStrategy_Plan_NewSubscription(t *testing.T) {
	s := &StickyGroupBalancer{}

	members := []GroupMember{}
	for i := 0; i < 3; i++ {
		topics := make([]string, 0)
		for j := i; j <= 3*i-2; j++ {
			topics = append(topics, fmt.Sprintf("topic%d", j))
		}
		members = append(members, GroupMember{ID: fmt.Sprintf("consumer%d", i), Topics: topics})
	}
	topics := []Partition{
		{Topic: "topic1",
			ID: 0},
		{Topic: "topic2",
			ID: 0},
		{Topic: "topic3",
			ID: 0},
		{Topic: "topic4",
			ID: 0},
		{Topic: "topic5",
			ID: 0},
	}
	plan1 := s.AssignGroups(members, topics)
	verifyValidityAndBalance(t, members, plan1)

	for _, member := range members {
		if member.ID == "consumer0" {
			member.Topics = []string{"topic1"}
		}
	}
	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
}

func Test_stickyBalanceStrategy_Plan_ReassignmentWithRandomSubscriptionsAndChanges(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	minNumConsumers := 20
	maxNumConsumers := 40
	minNumTopics := 10
	maxNumTopics := 20

	for round := 0; round < 100; round++ {
		numTopics := minNumTopics + r.Intn(maxNumTopics-minNumTopics)
		topics := make([]string, numTopics)
		partitionsPerTopic := []Partition{}
		for i := 0; i < numTopics; i++ {
			topicName := fmt.Sprintf("topic%d", i)
			topics[i] = topicName
			for j := 0; j < maxNumTopics; j++ {
				partitionsPerTopic = append(partitionsPerTopic, Partition{Topic: topicName, ID: j})
			}
		}

		numConsumers := minNumConsumers + r.Intn(maxNumConsumers-minNumConsumers)
		members := []GroupMember{}
		for i := 0; i < numConsumers; i++ {
			sub := getRandomSublist(r, topics)
			sort.Strings(sub)
			members = append(members, GroupMember{ID: fmt.Sprintf("consumer%d", i), Topics: topics})
		}

		s := &StickyGroupBalancer{}
		plan := s.AssignGroups(members, partitionsPerTopic)
		verifyPlanIsBalancedAndSticky(t, s, members, plan)

		// PLAN 2
		membersPlan2 := []GroupMember{}
		for i := 0; i < numConsumers; i++ {
			sub := getRandomSublist(r, topics)
			sort.Strings(sub)
			consumerId := fmt.Sprintf("consumer%d", i)
			topics32 := make(map[string][]int32)
			topics := plan[consumerId]
			for topic, partitions := range topics {
				partitions32 := make([]int32, len(partitions))
				for i := range partitions {
					partitions32[i] = int32(partitions[i])
				}
				topics32[topic] = partitions32
			}
			membersPlan2 = append(membersPlan2, GroupMember{ID: fmt.Sprintf("consumer%d", i),
				Topics: sub, UserData: encodeSubscriberPlan(t, topics32)})
		}

		plan2 := s.AssignGroups(membersPlan2, partitionsPerTopic)
		verifyPlanIsBalancedAndSticky(t, s, membersPlan2, plan2)
	}
}

func getRandomSublist(r *rand.Rand, s []string) []string {
	howManyToRemove := r.Intn(len(s))
	allEntriesMap := make(map[int]string)
	for i, s := range s {
		allEntriesMap[i] = s
	}
	for i := 0; i < howManyToRemove; i++ {
		delete(allEntriesMap, r.Intn(len(allEntriesMap)))
	}

	subList := make([]string, len(allEntriesMap))
	i := 0
	for _, s := range allEntriesMap {
		subList[i] = s
		i++
	}
	return subList
}

func Test_stickyBalanceStrategy_Plan_MoveExistingAssignments(t *testing.T) {
	s := &StickyGroupBalancer{}
	topics := []Partition{}
	for i := 1; i <= 6; i++ {
		topics = append(topics, Partition{Topic: fmt.Sprintf("topic%d", i), ID: 0})
	}
	members := []GroupMember{}
	members = append(members, GroupMember{ID: "consumer1", Topics: []string{"topic1", "topic2"}, UserData: encodeSubscriberPlan(t, map[string][]int32{"topic1": {0}})})
	members = append(members, GroupMember{ID: "consumer2", Topics: []string{"topic1", "topic2", "topic3", "topic4"}, UserData: encodeSubscriberPlan(t, map[string][]int32{"topic2": {0}, "topic3": {0}})})
	members = append(members, GroupMember{ID: "consumer3", Topics: []string{"topic2", "topic3", "topic4", "topic5", "topic6"}, UserData: encodeSubscriberPlan(t, map[string][]int32{"topic4": {0}, "topic5": {0}, "topic6": {0}})})

	plan := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan)
}

func Test_stickyBalanceStrategy_Plan_Stickiness(t *testing.T) {
	s := &StickyGroupBalancer{}

	topics := []Partition{}
	members := []GroupMember{
		{ID: "consumer1", Topics: []string{"topic1"}},
		{ID: "consumer2", Topics: []string{"topic1"}},
		{ID: "consumer3", Topics: []string{"topic1"}},
		{ID: "consumer4", Topics: []string{"topic1"}},
	}

	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	// PLAN 2
	// remove the potential group leader
	members = members[1:]
	for i := 2; i <= 4; i++ {
		consumerId := fmt.Sprintf("consumer%d", i)
		topics32 := make(map[string][]int32)
		topics := plan1[consumerId]
		for topic, partitions := range topics {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		members[i-2].UserData = encodeSubscriberPlan(t, topics32)

	}

	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
}

func Test_stickyBalanceStrategy_Plan_AssignmentUpdatedForDeletedTopic(t *testing.T) {
	s := &StickyGroupBalancer{}
	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
	}
	for i := 0; i < 100; i++ {
		topics = append(topics, Partition{Topic: "topic3", ID: i})
	}
	members := []GroupMember{
		{ID: "consumer1", Topics: []string{"topic1", "topic2", "topic3"}},
	}

	plan := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan)
	verifyFullyBalanced(t, plan)
	if (len(plan["consumer1"]["topic1"]) + len(plan["consumer1"]["topic3"])) != 101 {
		t.Error("Incorrect number of partitions assigned")
		return
	}
}

func Test_stickyBalanceStrategy_Plan_NoExceptionRaisedWhenOnlySubscribedTopicDeleted(t *testing.T) {
	s := &StickyGroupBalancer{}
	topics := []Partition{
		{Topic: "topic1",
			ID: 0},
		{Topic: "topic1",
			ID: 1},
		{Topic: "topic1",
			ID: 2},
	}
	members := []GroupMember{
		{ID: "consumer1", Topics: []string{"topic1"}},
	}
	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	// PLAN 2
	consumerId := "consumer1"
	topics32 := make(map[string][]int32)
	topicPartitions := plan1[consumerId]
	for topic, partitions := range topicPartitions {
		partitions32 := make([]int32, len(partitions))
		for i := range partitions {
			partitions32[i] = int32(partitions[i])
		}
		topics32[topic] = partitions32
	}
	members[0].UserData = encodeSubscriberPlan(t, topics32)

	plan2 := s.AssignGroups(members, []Partition{})
	if len(plan2) != 1 {
		t.Error("Incorrect number of consumers")
		return
	}
	if len(plan2["consumer1"]) != 0 {
		t.Error("Incorrect number of consumer topic assignments")
		return
	}
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithMultipleGenerations1(t *testing.T) {
	s := &StickyGroupBalancer{}
	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
		{
			Topic: "topic1",
			ID:    1,
		},
		{
			Topic: "topic1",
			ID:    2,
		},
		{
			Topic: "topic1",
			ID:    3,
		},
		{
			Topic: "topic1",
			ID:    4,
		},
		{
			Topic: "topic1",
			ID:    5,
		},
	}
	members := []GroupMember{
		{ID: "consumer1", Topics: []string{"topic1"}},
		{ID: "consumer2", Topics: []string{"topic1"}},
		{ID: "consumer3", Topics: []string{"topic1"}},
	}
	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)
	verifyFullyBalanced(t, plan1)

	// PLAN 2
	for ind, member := range members {
		topics32 := make(map[string][]int32)
		topicPartitions := plan1[member.ID]
		for topic, partitions := range topicPartitions {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		member.UserData = encodeSubscriberPlanWithGeneration(t, topics32, 1)
		members[ind] = member
	}
	members = members[:2]
	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
	verifyFullyBalanced(t, plan2)
	if len(intersection(plan1["consumer1"]["topic1"], plan2["consumer1"]["topic1"])) != 2 {
		t.Errorf("stickyBalanceStrategy.Plan() consumer1 didn't maintain partitions across reassignment,plan1:%v,\tplan2: %v \t members :%v", plan1, plan2, members)
	}
	if len(intersection(plan1["consumer2"]["topic1"], plan2["consumer2"]["topic1"])) != 2 {
		t.Error("stickyBalanceStrategy.Plan() consumer1 didn't maintain partitions across reassignment")
	}

	// PLAN 3
	members = members[1:]
	topics32 := make(map[string][]int32)
	topicPartitions := plan2["consumer2"]
	for topic, partitions := range topicPartitions {
		partitions32 := make([]int32, len(partitions))
		for i := range partitions {
			partitions32[i] = int32(partitions[i])
		}
		topics32[topic] = partitions32
	}
	members[0].UserData = encodeSubscriberPlanWithGeneration(t, topics32, 1)
	topics322 := make(map[string][]int32)
	tttopics := plan1["consumer3"]
	for topic, partitions := range tttopics {
		partitions32 := make([]int32, len(partitions))
		for i := range partitions {
			partitions32[i] = int32(partitions[i])
		}
		topics322[topic] = partitions32
	}
	members = append(members, GroupMember{ID: "consumer3", Topics: []string{"topic1"}, UserData: encodeSubscriberPlanWithGeneration(t, topics322, 1)})

	plan3 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan3)
	verifyFullyBalanced(t, plan3)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithMultipleGenerations2(t *testing.T) {
	s := &StickyGroupBalancer{}
	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
		{
			Topic: "topic1",
			ID:    1,
		},
		{
			Topic: "topic1",
			ID:    2,
		},
		{
			Topic: "topic1",
			ID:    3,
		},
		{
			Topic: "topic1",
			ID:    4,
		},
		{
			Topic: "topic1",
			ID:    5,
		},
	}
	members := []GroupMember{
		{ID: "consumer1", Topics: []string{"topic1"}},
		{ID: "consumer2", Topics: []string{"topic1"}},
		{ID: "consumer3", Topics: []string{"topic1"}},
	}
	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)
	verifyFullyBalanced(t, plan1)

	// PLAN 2
	members = members[1:]
	members = members[:1]
	topics32 := make(map[string][]int32)
	topicPartitions := plan1["consumer2"]
	for topic, partitions := range topicPartitions {
		partitions32 := make([]int32, len(partitions))
		for i := range partitions {
			partitions32[i] = int32(partitions[i])
		}
		topics32[topic] = partitions32
	}
	members[0].UserData = encodeSubscriberPlanWithGeneration(t, topics32, 1)

	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
	verifyFullyBalanced(t, plan2)
	if len(intersection(plan1["consumer2"]["topic1"], plan2["consumer2"]["topic1"])) != 2 {
		t.Error("stickyBalanceStrategy.Plan() consumer1 didn't maintain partitions across reassignment")
	}

	// PLAN 3
	topics2 := make(map[string][]int32)
	topicPartitions2 := plan1["consumer1"]
	for topic, partitions := range topicPartitions2 {
		partitions32 := make([]int32, len(partitions))
		for i := range partitions {
			partitions32[i] = int32(partitions[i])
		}
		topics2[topic] = partitions32
	}
	members = append(members, GroupMember{ID: "consumer1", Topics: []string{"topic1"}, UserData: encodeSubscriberPlanWithGeneration(t, topics2, 1)})
	topics3 := make(map[string][]int32)
	topicPartitions3 := plan1["consumer3"]
	for topic, partitions := range topicPartitions3 {
		partitions32 := make([]int32, len(partitions))
		for i := range partitions {
			partitions32[i] = int32(partitions[i])
		}
		topics3[topic] = partitions32
	}
	topics4 := make(map[string][]int32)
	topicPartitions4 := plan2["consumer2"]
	for topic, partitions := range topicPartitions4 {
		partitions32 := make([]int32, len(partitions))
		for i := range partitions {
			partitions32[i] = int32(partitions[i])
		}
		topics4[topic] = partitions32
	}
	members[0].UserData = encodeSubscriberPlanWithGeneration(t, topics4, 2)
	plan3 := s.AssignGroups(members, topics)
	// nil is err
	verifyPlanIsBalancedAndSticky(t, s, members, plan3)
	verifyFullyBalanced(t, plan3)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithConflictingPreviousGenerations(t *testing.T) {
	s := &StickyGroupBalancer{}
	topics := []Partition{
		{
			Topic: "topic1",
			ID:    0,
		},
		{
			Topic: "topic1",
			ID:    1,
		},
		{
			Topic: "topic1",
			ID:    2,
		},
		{
			Topic: "topic1",
			ID:    3,
		},
		{
			Topic: "topic1",
			ID:    4,
		},
		{
			Topic: "topic1",
			ID:    5,
		},
	}
	members := []GroupMember{}
	members = append(members, GroupMember{ID: "consumer1", Topics: []string{"topic1"}, UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {0, 1, 4}}, 1)})
	members = append(members, GroupMember{ID: "consumer2", Topics: []string{"topic1"}, UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {0, 2, 3}}, 1)})
	members = append(members, GroupMember{ID: "consumer3", Topics: []string{"topic1"}, UserData: encodeSubscriberPlanWithGeneration(t, map[string][]int32{"topic1": {3, 4, 5}}, 2)})

	plan := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan)
	verifyFullyBalanced(t, plan)
}

func Test_sortPartitionsByPotentialConsumerAssignments(t *testing.T) {
	type args struct {
		partition2AllPotentialConsumers map[topicPartitionAssignment][]string
	}
	tests := []struct {
		name string
		args args
		want []topicPartitionAssignment
	}{
		{
			name: "Single topic partition",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{
						Topic:     "t1",
						Partition: 0,
					}: {"c1", "c2"},
				},
			},
			want: []topicPartitionAssignment{
				{
					Topic:     "t1",
					Partition: 0,
				},
			},
		},
		{
			name: "Multiple topic partitions with the same number of consumers but different topic names",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{
						Topic:     "t1",
						Partition: 0,
					}: {"c1", "c2"},
					{
						Topic:     "t2",
						Partition: 0,
					}: {"c1", "c2"},
				},
			},
			want: []topicPartitionAssignment{
				{
					Topic:     "t1",
					Partition: 0,
				},
				{
					Topic:     "t2",
					Partition: 0,
				},
			},
		},
		{
			name: "Multiple topic partitions with the same number of consumers and topic names",
			args: args{
				partition2AllPotentialConsumers: map[topicPartitionAssignment][]string{
					{
						Topic:     "t1",
						Partition: 0,
					}: {"c1", "c2"},
					{
						Topic:     "t1",
						Partition: 1,
					}: {"c1", "c2"},
				},
			},
			want: []topicPartitionAssignment{
				{
					Topic:     "t1",
					Partition: 0,
				},
				{
					Topic:     "t1",
					Partition: 1,
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := sortPartitionsByPotentialConsumerAssignments(tt.args.partition2AllPotentialConsumers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sortPartitionsByPotentialConsumerAssignments() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_stickyBalanceStrategy_Plan_ReassignmentAfterOneConsumerLeaves(t *testing.T) {
	s := &StickyGroupBalancer{}

	// PLAN 1
	members := []GroupMember{}
	for i := 0; i < 20; i++ {
		topics := make([]string, 20)
		for j := 0; j < 20; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members = append(members, GroupMember{ID: fmt.Sprintf("consumer%d", i), Topics: topics})
	}
	topics := []Partition{}
	for i := 0; i < 20; i++ {
		topic := fmt.Sprintf("topic%d", i)
		for j := 0; j < 20; j++ {
			topics = append(topics, Partition{Topic: topic, ID: j})
		}
	}

	plan1 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan1)

	for i := 0; i < 20; i++ {
		topics := make([]string, 20)
		for j := 0; j < 20; j++ {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		consumerid := fmt.Sprintf("consumer%d", i)
		topics32 := make(map[string][]int32)
		topicPartitions := plan1[consumerid]
		for topic, partitions := range topicPartitions {
			partitions32 := make([]int32, len(partitions))
			for i := range partitions {
				partitions32[i] = int32(partitions[i])
			}
			topics32[topic] = partitions32
		}
		members[i].UserData = encodeSubscriberPlan(t, topics32)
	}
	members = append(members[:10], members[11:]...)
	plan2 := s.AssignGroups(members, topics)
	verifyPlanIsBalancedAndSticky(t, s, members, plan2)
}
