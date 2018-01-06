package kafka

import "sort"

// strategy encapsulates the client side rebalancing logic
type strategy interface {
	// ProtocolName of strategy
	ProtocolName() string

	// ProtocolMetadata provides the strategy an opportunity to embed custom
	// UserData into the metadata.
	//
	// Will be used by JoinGroup to begin the consumer group handshake.
	//
	// See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-JoinGroupRequest
	GroupMetadata(topics []string) (groupMetadata, error)

	// DefineMemberships returns which members will be consuming
	// which topic partitions
	AssignGroups(members []memberGroupMetadata, partitions []Partition) memberGroupAssignments
}

var (
	// allStrategies the kafka-go Reader supports
	allStrategies = []strategy{
		rangeStrategy{},
		roundrobinStrategy{},
	}
)

// rangeStrategy groups consumers by partition
//
// Example: 5 partitions, 2 consumers
// 		C0: [0, 1, 2]
// 		C1: [3, 4]
//
// Example: 6 partitions, 3 consumers
// 		C0: [0, 1]
// 		C1: [2, 3]
// 		C2: [4, 5]
//
type rangeStrategy struct{}

func (r rangeStrategy) ProtocolName() string {
	return "range"
}

func (r rangeStrategy) GroupMetadata(topics []string) (groupMetadata, error) {
	return groupMetadata{
		Version: 1,
		Topics:  topics,
	}, nil
}

func (r rangeStrategy) AssignGroups(members []memberGroupMetadata, topicPartitions []Partition) memberGroupAssignments {
	groupAssignments := memberGroupAssignments{}
	membersByTopic := findMembersByTopic(members)

	for topic, members := range membersByTopic {
		partitions := findPartitions(topic, topicPartitions)
		partitionCount := len(partitions)
		memberCount := len(members)

		rangeSize := partitionCount / memberCount
		if partitionCount%memberCount != 0 {
			rangeSize++
		}

		for memberIndex, member := range members {
			assignmentsByTopic, ok := groupAssignments[member.MemberID]
			if !ok {
				assignmentsByTopic = map[string][]int32{}
				groupAssignments[member.MemberID] = assignmentsByTopic
			}

			for partitionIndex, partition := range partitions {
				if (partitionIndex / rangeSize) == memberIndex {
					assignmentsByTopic[topic] = append(assignmentsByTopic[topic], partition)
				}
			}
		}
	}

	return groupAssignments
}

// roundrobinStrategy divides partitions evenly among consumers
//
// Example: 5 partitions, 2 consumers
// 		C0: [0, 2, 4]
// 		C1: [1, 3]
//
// Example: 6 partitions, 3 consumers
// 		C0: [0, 3]
// 		C1: [1, 4]
// 		C2: [2, 5]
//
type roundrobinStrategy struct{}

func (r roundrobinStrategy) ProtocolName() string {
	return "roundrobin"
}

func (r roundrobinStrategy) GroupMetadata(topics []string) (groupMetadata, error) {
	return groupMetadata{
		Version: 1,
		Topics:  topics,
	}, nil
}

func (r roundrobinStrategy) AssignGroups(members []memberGroupMetadata, topicPartitions []Partition) memberGroupAssignments {
	groupAssignments := memberGroupAssignments{}
	membersByTopic := findMembersByTopic(members)
	for topic, members := range membersByTopic {
		partitionIDs := findPartitions(topic, topicPartitions)
		memberCount := len(members)

		for memberIndex, member := range members {
			assignmentsByTopic, ok := groupAssignments[member.MemberID]
			if !ok {
				assignmentsByTopic = map[string][]int32{}
				groupAssignments[member.MemberID] = assignmentsByTopic
			}

			for partitionIndex, partition := range partitionIDs {
				if (partitionIndex % memberCount) == memberIndex {
					assignmentsByTopic[topic] = append(assignmentsByTopic[topic], partition)
				}
			}
		}
	}

	return groupAssignments
}

// findPartitions extracts the partition ids associated with the topic from the
// list of Partitions provided
func findPartitions(topic string, partitions []Partition) []int32 {
	var ids []int32
	for _, partition := range partitions {
		if partition.Topic == topic {
			ids = append(ids, int32(partition.ID))
		}
	}
	return ids
}

// findMembersByTopic groups the memberGroupMetadata by topic
func findMembersByTopic(members []memberGroupMetadata) map[string][]memberGroupMetadata {
	membersByTopic := map[string][]memberGroupMetadata{}
	for _, member := range members {
		for _, topic := range member.Metadata.Topics {
			membersByTopic[topic] = append(membersByTopic[topic], member)
		}
	}

	// normalize ordering of members to enabling grouping across topics by partitions
	//
	// Want:
	// 		C0 [T0/P0, T1/P0]
	// 		C1 [T0/P1, T1/P1]
	//
	// Not:
	// 		C0 [T0/P0, T1/P1]
	// 		C1 [T0/P1, T1/P0]
	//
	// Even though the later is still round robin, the partitions are crossed
	//
	for _, members := range membersByTopic {
		sort.Slice(members, func(i, j int) bool {
			return members[i].MemberID < members[j].MemberID
		})
	}

	return membersByTopic
}

// findStrategy returns the strategy with the specified protocolName from the
// slice provided
func findStrategy(protocolName string, strategies []strategy) (strategy, bool) {
	for _, strategy := range strategies {
		if strategy.ProtocolName() == protocolName {
			return strategy, true
		}
	}
	return nil, false
}
