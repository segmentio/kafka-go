package kafka

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
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
			zone, err := b.UserData()
			if err != nil {
				t.Fatal(err)
			}
			if string(zone) != "" {
				t.Fatalf("expected empty zone but got %s", zone)
			}
		})

		t.Run("configure zone", func(t *testing.T) {
			b := RackAffinityGroupBalancer{Rack: "zone1"}
			zone, err := b.UserData()
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
