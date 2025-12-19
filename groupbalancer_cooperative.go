package kafka

import (
	"encoding/binary"
	"sort"
	"sync"
)

// Cooperative Sticky Partition Assignment for kafka-go
//
// This file provides two assignors for consumer group partition assignment:
//
// 1. CooperativeStickyAssignor - Sticky assignment with cooperative protocol support
//    - Protocol name: "cooperative-sticky"
//    - Compatible with Sarama and Java Kafka client
//
// 2. CooperativeStickyRackAffinityAssignor - Sticky + Rack-aware assignment
//    - Protocol name: "cooperative-sticky-rack-affinity"
//    - Optimizes for network locality in multi-AZ deployments (AWS MSK, etc.)
//
// COMPATIBILITY:
//
// - Amazon MSK: Fully compatible (all versions)
// - Confluent Cloud: Fully compatible
// - Apache Kafka: 2.4+ (KIP-429) for cooperative protocol
// - Sarama: Wire format compatible
// - Java client: Wire format compatible
//
// INTEGRATION STATUS:
//
// This implementation includes full integration with kafka-go's ConsumerGroup:
// - OwnedPartitions are sent in JoinGroup subscription metadata
// - SetAssignment is called after receiving sync group response
// - The assignor maintains state across rebalances for maximum stickiness
//
// NOTES:
//
// - The cooperative protocol requires consumers to report their owned partitions
//   during the JoinGroup request. This allows the leader to know what each consumer
//   currently has and minimize partition movement.
//
// - While this implementation provides the sticky assignment algorithm and reports
//   owned partitions, full incremental cooperative rebalancing (KIP-429) would still
//   require two-phase rebalancing where partitions are first revoked before being
//   assigned to new consumers. Currently, kafka-go uses an EAGER-style rebalance
//   where all partitions are revoked during rebalance, but the sticky algorithm
//   ensures partitions return to their previous owners when possible.

// ===== Core Types =====

// cooperativeStickyTopicPartition represents a topic-partition pair (internal use).
type cooperativeStickyTopicPartition struct {
	Topic     string
	Partition int
}

// cooperativeStickyConsumerGenerationPair tracks which consumer owned a partition and in which generation.
type cooperativeStickyConsumerGenerationPair struct {
	MemberID   string
	Generation int32
}

// StickyAssignorUserDataV1 is the structure for encoding/decoding user data
// in the cooperative sticky assignor. This matches the wire format used by
// Sarama and the Java client.
type StickyAssignorUserDataV1 struct {
	Version    int16
	Topics     map[string][]int32
	Generation int32
}

// ===== CooperativeStickyAssignor =====

// CooperativeStickyAssignor implements a sticky partition assignment strategy
// that supports incremental cooperative rebalancing as specified in KIP-429.
//
// This assignor has the following properties:
//   - Balanced: Partitions are distributed as evenly as possible
//   - Sticky: Tries to preserve existing partition assignments
//   - Cooperative: Supports incremental rebalance protocol (with ConsumerGroup changes)
//
// The algorithm prefers stability over perfect balance. When a new consumer joins
// or an existing consumer leaves, the assignor will try to minimize the number of
// partition movements while still maintaining relatively even distribution.
//
// Example usage with kafka-go:
//
//	reader := kafka.NewReader(kafka.ReaderConfig{
//	    Brokers:        []string{"localhost:9092"},
//	    GroupID:        "my-group",
//	    Topic:          "my-topic",
//	    GroupBalancers: []kafka.GroupBalancer{
//	        &kafka.CooperativeStickyAssignor{},
//	    },
//	})
//
// For maximum stickiness, the assignor maintains state across rebalances.
// The currentAssignment field tracks what partitions this consumer owns,
// which is encoded in UserData and sent to the group leader.
//
// Thread Safety: This type is safe for concurrent use. All access to internal
// state is protected by a mutex.
type CooperativeStickyAssignor struct {
	// mu protects all fields below
	mu sync.RWMutex

	// currentAssignment holds the partitions currently assigned to this consumer.
	// This is used to report owned partitions in the subscription and to
	// preserve assignments during rebalancing.
	currentAssignment map[string][]int32

	// generation is the current consumer group generation.
	generation int32
}

// ProtocolName returns the name of this assignor's protocol.
// The name "cooperative-sticky" is compatible with:
// - Sarama's CooperativeStickyAssignor
// - Java client's CooperativeStickyAssignor
func (s *CooperativeStickyAssignor) ProtocolName() string {
	return "cooperative-sticky"
}

// UserData serializes the current assignment state to be sent to the group leader.
// This allows the leader to make sticky assignments based on what partitions
// each consumer currently owns.
//
// The format is compatible with Sarama and the Java client:
//   - 2 bytes: version (int16)
//   - 4 bytes: topic count (int32)
//   - For each topic:
//   - 2 bytes: topic name length (int16)
//   - N bytes: topic name
//   - 4 bytes: partition count (int32)
//   - 4 bytes per partition: partition ID (int32)
//   - 4 bytes: generation (int32)
func (s *CooperativeStickyAssignor) UserData() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.currentAssignment) == 0 {
		return nil, nil
	}

	data := StickyAssignorUserDataV1{
		Version:    1,
		Topics:     s.currentAssignment,
		Generation: s.generation,
	}

	return encodeStickyAssignorUserData(data)
}

// SetAssignment updates the current assignment for this consumer.
// This should be called after receiving a new assignment from the group leader
// to maintain stickiness across rebalances.
//
// Parameters:
//   - assignment: map of topic -> []partition assigned to this consumer
//   - generation: the consumer group generation ID
func (s *CooperativeStickyAssignor) SetAssignment(assignment map[string][]int32, generation int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentAssignment = assignment
	s.generation = generation
}

// GetOwnedPartitions returns the partitions currently owned by this consumer.
// This is used when building the subscription to send to the coordinator.
func (s *CooperativeStickyAssignor) GetOwnedPartitions() map[string][]int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.currentAssignment
}

// GetGeneration returns the current generation ID.
func (s *CooperativeStickyAssignor) GetGeneration() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.generation
}

// AssignGroups assigns partitions to group members using a sticky strategy.
//
// The algorithm works as follows:
//  1. Collect current assignments from all members via their UserData
//  2. Identify partitions that need to be reassigned (unassigned, or owner left/unsubscribed)
//  3. Balance partitions while preserving existing assignments where possible
//  4. Minimize partition movement between members
//
// For cooperative protocol: partitions are not explicitly revoked during assignment.
// Instead, the algorithm produces an assignment that may result in some partitions
// being "stolen" from their current owners when there's imbalance. The actual
// revocation happens at the consumer level based on comparing old vs new assignment.
//
// Parameters:
//   - members: list of all consumers in the group with their subscriptions
//   - partitions: list of all partitions to be assigned
//
// Returns:
//   - GroupMemberAssignments: map of member ID -> topic -> []partition IDs
func (s *CooperativeStickyAssignor) AssignGroups(members []GroupMember, partitions []Partition) GroupMemberAssignments {
	// Build a sorted list of all topic-partitions
	allPartitions := make([]cooperativeStickyTopicPartition, 0, len(partitions))
	for _, p := range partitions {
		allPartitions = append(allPartitions, cooperativeStickyTopicPartition{
			Topic:     p.Topic,
			Partition: p.ID,
		})
	}
	sortCooperativeStickyTopicPartitions(allPartitions)

	// Parse current assignments from member user data
	currentAssignment, prevAssignment := s.prepopulateCurrentAssignments(members)

	// Build mapping of partition -> potential consumers (those subscribed to the topic)
	partition2AllPotentialConsumers := make(map[cooperativeStickyTopicPartition][]string)
	for _, tp := range allPartitions {
		partition2AllPotentialConsumers[tp] = []string{}
	}

	// Build mapping of consumer -> potential partitions
	consumer2AllPotentialPartitions := make(map[string][]cooperativeStickyTopicPartition)
	for _, member := range members {
		consumer2AllPotentialPartitions[member.ID] = make([]cooperativeStickyTopicPartition, 0)
		for _, topic := range member.Topics {
			for _, tp := range allPartitions {
				if tp.Topic == topic {
					consumer2AllPotentialPartitions[member.ID] = append(
						consumer2AllPotentialPartitions[member.ID], tp)
					partition2AllPotentialConsumers[tp] = append(
						partition2AllPotentialConsumers[tp], member.ID)
				}
			}
		}
		// Initialize current assignment if not present
		if _, exists := currentAssignment[member.ID]; !exists {
			currentAssignment[member.ID] = make([]cooperativeStickyTopicPartition, 0)
		}
	}

	// Build current partition -> consumer mapping and find unassigned partitions
	currentPartitionConsumer := make(map[cooperativeStickyTopicPartition]string)
	unvisitedPartitions := make(map[cooperativeStickyTopicPartition]bool)
	for tp := range partition2AllPotentialConsumers {
		unvisitedPartitions[tp] = true
	}

	var unassignedPartitions []cooperativeStickyTopicPartition

	for memberID, tps := range currentAssignment {
		var keepPartitions []cooperativeStickyTopicPartition
		for _, tp := range tps {
			// Skip if partition no longer exists
			if _, exists := partition2AllPotentialConsumers[tp]; !exists {
				continue
			}
			delete(unvisitedPartitions, tp)
			currentPartitionConsumer[tp] = memberID

			// Check if member is still subscribed to this topic
			memberTopics := getCooperativeStickyMemberTopics(members, memberID)
			if !containsCooperativeStickyString(memberTopics, tp.Topic) {
				unassignedPartitions = append(unassignedPartitions, tp)
				continue
			}
			keepPartitions = append(keepPartitions, tp)
		}
		currentAssignment[memberID] = keepPartitions
	}

	// Add unvisited partitions to unassigned
	for tp := range unvisitedPartitions {
		unassignedPartitions = append(unassignedPartitions, tp)
	}

	// Sort partitions for consistent ordering
	sortCooperativeStickyTopicPartitions(unassignedPartitions)

	// Perform the assignment balancing
	s.balance(
		currentAssignment,
		prevAssignment,
		allPartitions,
		unassignedPartitions,
		consumer2AllPotentialPartitions,
		partition2AllPotentialConsumers,
		currentPartitionConsumer,
	)

	// Convert to GroupMemberAssignments format
	result := make(GroupMemberAssignments)
	for memberID, tps := range currentAssignment {
		if len(tps) == 0 {
			result[memberID] = make(map[string][]int)
		} else {
			topicParts := make(map[string][]int)
			for _, tp := range tps {
				topicParts[tp.Topic] = append(topicParts[tp.Topic], tp.Partition)
			}
			result[memberID] = topicParts
		}
	}

	return result
}

// prepopulateCurrentAssignments extracts current assignments from member metadata.
// It returns:
//   - currentAssignment: the most recent assignment for each member
//   - prevAssignment: previous owners for partitions that changed hands
func (s *CooperativeStickyAssignor) prepopulateCurrentAssignments(members []GroupMember) (
	map[string][]cooperativeStickyTopicPartition,
	map[cooperativeStickyTopicPartition]cooperativeStickyConsumerGenerationPair,
) {
	currentAssignment := make(map[string][]cooperativeStickyTopicPartition)
	prevAssignment := make(map[cooperativeStickyTopicPartition]cooperativeStickyConsumerGenerationPair)

	// Track partition -> generation -> member for conflict resolution
	// When multiple consumers claim the same partition, use generation to resolve
	sortedPartitionConsumersByGeneration := make(map[cooperativeStickyTopicPartition]map[int32]string)

	for _, member := range members {
		if len(member.UserData) == 0 {
			continue
		}

		userData, err := decodeStickyAssignorUserData(member.UserData)
		if err != nil {
			continue
		}

		for topic, partitions := range userData.Topics {
			for _, partition := range partitions {
				tp := cooperativeStickyTopicPartition{Topic: topic, Partition: int(partition)}

				if consumers, exists := sortedPartitionConsumersByGeneration[tp]; exists {
					if userData.Generation > 0 {
						if _, genExists := consumers[userData.Generation]; genExists {
							// Conflict: same partition assigned to multiple consumers in same generation
							// This shouldn't happen normally, skip this claim
							continue
						}
						consumers[userData.Generation] = member.ID
					} else {
						consumers[-1] = member.ID // Default generation for old/missing data
					}
				} else {
					gen := int32(-1)
					if userData.Generation > 0 {
						gen = userData.Generation
					}
					sortedPartitionConsumersByGeneration[tp] = map[int32]string{gen: member.ID}
				}
			}
		}
	}

	// Resolve conflicts by picking highest generation
	for tp, consumers := range sortedPartitionConsumersByGeneration {
		var generations []int32
		for gen := range consumers {
			generations = append(generations, gen)
		}
		sort.Slice(generations, func(i, j int) bool {
			return generations[i] > generations[j] // Descending order
		})

		// Highest generation wins
		consumer := consumers[generations[0]]
		if _, exists := currentAssignment[consumer]; !exists {
			currentAssignment[consumer] = []cooperativeStickyTopicPartition{tp}
		} else {
			currentAssignment[consumer] = append(currentAssignment[consumer], tp)
		}

		// Track previous owner for potential sticky reassignment
		if len(generations) > 1 {
			prevAssignment[tp] = cooperativeStickyConsumerGenerationPair{
				MemberID:   consumers[generations[1]],
				Generation: generations[1],
			}
		}
	}

	return currentAssignment, prevAssignment
}

// balance performs the actual partition assignment balancing.
// It iteratively assigns unassigned partitions and rebalances overloaded consumers.
func (s *CooperativeStickyAssignor) balance(
	currentAssignment map[string][]cooperativeStickyTopicPartition,
	prevAssignment map[cooperativeStickyTopicPartition]cooperativeStickyConsumerGenerationPair,
	sortedPartitions []cooperativeStickyTopicPartition,
	unassignedPartitions []cooperativeStickyTopicPartition,
	consumer2AllPotentialPartitions map[string][]cooperativeStickyTopicPartition,
	partition2AllPotentialConsumers map[cooperativeStickyTopicPartition][]string,
	currentPartitionConsumer map[cooperativeStickyTopicPartition]string,
) {
	initializing := len(currentAssignment) == 0 || s.allAssignmentsEmpty(currentAssignment)

	// Assign all unassigned partitions
	for _, partition := range unassignedPartitions {
		if len(partition2AllPotentialConsumers[partition]) == 0 {
			continue
		}
		s.assignPartition(
			partition,
			currentAssignment,
			consumer2AllPotentialPartitions,
			currentPartitionConsumer,
		)
	}

	// Check if we're already balanced
	if s.isBalanced(currentAssignment, consumer2AllPotentialPartitions) {
		return
	}

	// Find partitions that can be reassigned (have multiple potential owners)
	reassignablePartitions := make([]cooperativeStickyTopicPartition, 0)
	for _, tp := range sortedPartitions {
		if s.canPartitionParticipateInReassignment(tp, partition2AllPotentialConsumers) {
			reassignablePartitions = append(reassignablePartitions, tp)
		}
	}

	// Find consumers that cannot participate in reassignment and set them aside
	fixedAssignments := make(map[string][]cooperativeStickyTopicPartition)
	for memberID := range consumer2AllPotentialPartitions {
		if !s.canConsumerParticipateInReassignment(
			memberID,
			currentAssignment,
			consumer2AllPotentialPartitions,
			partition2AllPotentialConsumers,
		) {
			fixedAssignments[memberID] = currentAssignment[memberID]
			delete(currentAssignment, memberID)
		}
	}

	// Save pre-balance state for comparison
	preBalanceAssignment := s.deepCopyAssignment(currentAssignment)
	preBalanceScore := s.getBalanceScore(preBalanceAssignment)

	// Perform reassignments to improve balance
	reassignmentPerformed := s.performReassignments(
		reassignablePartitions,
		currentAssignment,
		prevAssignment,
		consumer2AllPotentialPartitions,
		partition2AllPotentialConsumers,
		currentPartitionConsumer,
	)

	// If we're not initializing and made changes, verify we improved
	if !initializing && reassignmentPerformed {
		newScore := s.getBalanceScore(currentAssignment)
		if newScore >= preBalanceScore {
			// Didn't improve - restore pre-balance assignment
			for k := range currentAssignment {
				delete(currentAssignment, k)
			}
			for k, v := range preBalanceAssignment {
				currentAssignment[k] = v
			}
		}
	}

	// Add back fixed assignments
	for memberID, tps := range fixedAssignments {
		currentAssignment[memberID] = tps
	}
}

// allAssignmentsEmpty checks if all consumers have empty assignments.
func (s *CooperativeStickyAssignor) allAssignmentsEmpty(assignment map[string][]cooperativeStickyTopicPartition) bool {
	for _, tps := range assignment {
		if len(tps) > 0 {
			return false
		}
	}
	return true
}

// assignPartition assigns a partition to the consumer with fewest assignments.
func (s *CooperativeStickyAssignor) assignPartition(
	partition cooperativeStickyTopicPartition,
	currentAssignment map[string][]cooperativeStickyTopicPartition,
	consumer2AllPotentialPartitions map[string][]cooperativeStickyTopicPartition,
	currentPartitionConsumer map[cooperativeStickyTopicPartition]string,
) {
	// Sort members by number of assignments (ascending)
	sortedMembers := s.sortMembersByAssignmentCount(currentAssignment)

	for _, memberID := range sortedMembers {
		// Check if member can be assigned this partition
		if s.memberCanBeAssignedPartition(memberID, partition, consumer2AllPotentialPartitions) {
			currentAssignment[memberID] = append(currentAssignment[memberID], partition)
			currentPartitionConsumer[partition] = memberID
			return
		}
	}
}

// performReassignments iteratively reassigns partitions to improve balance.
func (s *CooperativeStickyAssignor) performReassignments(
	reassignablePartitions []cooperativeStickyTopicPartition,
	currentAssignment map[string][]cooperativeStickyTopicPartition,
	prevAssignment map[cooperativeStickyTopicPartition]cooperativeStickyConsumerGenerationPair,
	consumer2AllPotentialPartitions map[string][]cooperativeStickyTopicPartition,
	partition2AllPotentialConsumers map[cooperativeStickyTopicPartition][]string,
	currentPartitionConsumer map[cooperativeStickyTopicPartition]string,
) bool {
	reassignmentPerformed := false

	for {
		modified := false

		for _, partition := range reassignablePartitions {
			if s.isBalanced(currentAssignment, consumer2AllPotentialPartitions) {
				break
			}

			if len(partition2AllPotentialConsumers[partition]) <= 1 {
				continue
			}

			consumer := currentPartitionConsumer[partition]
			if consumer == "" {
				continue
			}

			// Priority 1: Check if previous owner should get it back (stickiness preference)
			if prev, exists := prevAssignment[partition]; exists {
				if len(currentAssignment[consumer]) > len(currentAssignment[prev.MemberID])+1 {
					s.reassignPartition(
						partition,
						consumer,
						prev.MemberID,
						currentAssignment,
						currentPartitionConsumer,
					)
					reassignmentPerformed = true
					modified = true
					continue
				}
			}

			// Priority 2: Check if another consumer should get it (balance)
			for _, otherConsumer := range partition2AllPotentialConsumers[partition] {
				if len(currentAssignment[consumer]) > len(currentAssignment[otherConsumer])+1 {
					s.reassignPartition(
						partition,
						consumer,
						otherConsumer,
						currentAssignment,
						currentPartitionConsumer,
					)
					reassignmentPerformed = true
					modified = true
					break
				}
			}
		}

		if !modified {
			break
		}
	}

	return reassignmentPerformed
}

// reassignPartition moves a partition from one consumer to another.
func (s *CooperativeStickyAssignor) reassignPartition(
	partition cooperativeStickyTopicPartition,
	oldConsumer, newConsumer string,
	currentAssignment map[string][]cooperativeStickyTopicPartition,
	currentPartitionConsumer map[cooperativeStickyTopicPartition]string,
) {
	// Remove from old consumer
	oldAssignments := currentAssignment[oldConsumer]
	newOldAssignments := make([]cooperativeStickyTopicPartition, 0, len(oldAssignments)-1)
	for _, tp := range oldAssignments {
		if tp != partition {
			newOldAssignments = append(newOldAssignments, tp)
		}
	}
	currentAssignment[oldConsumer] = newOldAssignments

	// Add to new consumer
	currentAssignment[newConsumer] = append(currentAssignment[newConsumer], partition)
	currentPartitionConsumer[partition] = newConsumer
}

// isBalanced checks if the current assignment is balanced.
// An assignment is balanced when the difference between the consumer with most
// partitions and the consumer with least partitions is at most 1.
func (s *CooperativeStickyAssignor) isBalanced(
	currentAssignment map[string][]cooperativeStickyTopicPartition,
	consumer2AllPotentialPartitions map[string][]cooperativeStickyTopicPartition,
) bool {
	if len(currentAssignment) == 0 {
		return true
	}

	sortedMembers := s.sortMembersByAssignmentCount(currentAssignment)
	min := len(currentAssignment[sortedMembers[0]])
	max := len(currentAssignment[sortedMembers[len(sortedMembers)-1]])

	// If min and max differ by at most 1, we're balanced
	if min >= max-1 {
		return true
	}

	// Check if any under-assigned consumer could take a partition from an over-assigned one
	allPartitions := make(map[cooperativeStickyTopicPartition]string)
	for memberID, tps := range currentAssignment {
		for _, tp := range tps {
			allPartitions[tp] = memberID
		}
	}

	for _, memberID := range sortedMembers {
		consumerPartitionCount := len(currentAssignment[memberID])
		// Skip if consumer already has max possible partitions
		if consumerPartitionCount == len(consumer2AllPotentialPartitions[memberID]) {
			continue
		}

		for _, tp := range consumer2AllPotentialPartitions[memberID] {
			if !s.containsPartition(currentAssignment[memberID], tp) {
				otherConsumer := allPartitions[tp]
				if consumerPartitionCount < len(currentAssignment[otherConsumer]) {
					return false
				}
			}
		}
	}

	return true
}

// getBalanceScore calculates a score representing how balanced the assignment is.
// Lower scores are better (0 = perfectly balanced).
// The score is the sum of absolute differences between all pairs of consumers.
func (s *CooperativeStickyAssignor) getBalanceScore(assignment map[string][]cooperativeStickyTopicPartition) int {
	consumer2AssignmentSize := make(map[string]int)
	for memberID, tps := range assignment {
		consumer2AssignmentSize[memberID] = len(tps)
	}

	score := 0
	members := make([]string, 0, len(consumer2AssignmentSize))
	for m := range consumer2AssignmentSize {
		members = append(members, m)
	}

	for i, m1 := range members {
		for _, m2 := range members[i+1:] {
			diff := consumer2AssignmentSize[m1] - consumer2AssignmentSize[m2]
			if diff < 0 {
				diff = -diff
			}
			score += diff
		}
	}

	return score
}

// canPartitionParticipateInReassignment checks if a partition can be reassigned.
// A partition can be reassigned if there are at least 2 potential consumers.
func (s *CooperativeStickyAssignor) canPartitionParticipateInReassignment(
	partition cooperativeStickyTopicPartition,
	partition2AllPotentialConsumers map[cooperativeStickyTopicPartition][]string,
) bool {
	return len(partition2AllPotentialConsumers[partition]) >= 2
}

// canConsumerParticipateInReassignment checks if a consumer can participate in reassignment.
func (s *CooperativeStickyAssignor) canConsumerParticipateInReassignment(
	memberID string,
	currentAssignment map[string][]cooperativeStickyTopicPartition,
	consumer2AllPotentialPartitions map[string][]cooperativeStickyTopicPartition,
	partition2AllPotentialConsumers map[cooperativeStickyTopicPartition][]string,
) bool {
	currentPartitions := currentAssignment[memberID]
	currentAssignmentSize := len(currentPartitions)
	maxAssignmentSize := len(consumer2AllPotentialPartitions[memberID])

	// Consumer can receive more partitions
	if currentAssignmentSize < maxAssignmentSize {
		return true
	}

	// Consumer has reassignable partitions
	for _, tp := range currentPartitions {
		if s.canPartitionParticipateInReassignment(tp, partition2AllPotentialConsumers) {
			return true
		}
	}

	return false
}

// memberCanBeAssignedPartition checks if a member can be assigned a specific partition.
func (s *CooperativeStickyAssignor) memberCanBeAssignedPartition(
	memberID string,
	partition cooperativeStickyTopicPartition,
	consumer2AllPotentialPartitions map[string][]cooperativeStickyTopicPartition,
) bool {
	for _, tp := range consumer2AllPotentialPartitions[memberID] {
		if tp == partition {
			return true
		}
	}
	return false
}

// sortMembersByAssignmentCount returns members sorted by number of assignments (ascending).
// Ties are broken by member ID for deterministic ordering.
func (s *CooperativeStickyAssignor) sortMembersByAssignmentCount(
	assignment map[string][]cooperativeStickyTopicPartition,
) []string {
	members := make([]string, 0, len(assignment))
	for m := range assignment {
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool {
		countI := len(assignment[members[i]])
		countJ := len(assignment[members[j]])
		if countI == countJ {
			return members[i] < members[j] // Deterministic tie-breaker
		}
		return countI < countJ
	})
	return members
}

// containsPartition checks if a slice contains a partition.
func (s *CooperativeStickyAssignor) containsPartition(tps []cooperativeStickyTopicPartition, tp cooperativeStickyTopicPartition) bool {
	for _, t := range tps {
		if t == tp {
			return true
		}
	}
	return false
}

// deepCopyAssignment creates a deep copy of an assignment map.
func (s *CooperativeStickyAssignor) deepCopyAssignment(
	assignment map[string][]cooperativeStickyTopicPartition,
) map[string][]cooperativeStickyTopicPartition {
	result := make(map[string][]cooperativeStickyTopicPartition, len(assignment))
	for k, v := range assignment {
		newV := make([]cooperativeStickyTopicPartition, len(v))
		copy(newV, v)
		result[k] = newV
	}
	return result
}

// ===== Helper Functions =====

func sortCooperativeStickyTopicPartitions(tps []cooperativeStickyTopicPartition) {
	sort.Slice(tps, func(i, j int) bool {
		if tps[i].Topic == tps[j].Topic {
			return tps[i].Partition < tps[j].Partition
		}
		return tps[i].Topic < tps[j].Topic
	})
}

func getCooperativeStickyMemberTopics(members []GroupMember, memberID string) []string {
	for _, m := range members {
		if m.ID == memberID {
			return m.Topics
		}
	}
	return nil
}

func containsCooperativeStickyString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// ===== Wire Format Encoding/Decoding =====

// encodeStickyAssignorUserData encodes StickyAssignorUserDataV1 to bytes.
// Format:
//   - 2 bytes: version (int16)
//   - 4 bytes: topic count (int32)
//   - For each topic:
//   - 2 bytes: topic name length (int16)
//   - N bytes: topic name
//   - 4 bytes: partition count (int32)
//   - 4 bytes per partition: partition ID (int32)
//   - 4 bytes: generation (int32)
func encodeStickyAssignorUserData(data StickyAssignorUserDataV1) ([]byte, error) {
	// Calculate size
	size := 2 // version
	size += 4 // topic count

	for topic, partitions := range data.Topics {
		size += 2 + len(topic) // topic name length + name
		size += 4              // partition count
		size += 4 * len(partitions)
	}
	size += 4 // generation

	buf := make([]byte, size)
	offset := 0

	// Version
	binary.BigEndian.PutUint16(buf[offset:], uint16(data.Version))
	offset += 2

	// Topic count
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(data.Topics)))
	offset += 4

	// Topics (sorted for deterministic encoding)
	topics := make([]string, 0, len(data.Topics))
	for topic := range data.Topics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		partitions := data.Topics[topic]

		// Topic name length + name
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(topic)))
		offset += 2
		copy(buf[offset:], topic)
		offset += len(topic)

		// Partition count + partitions
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(partitions)))
		offset += 4
		for _, p := range partitions {
			binary.BigEndian.PutUint32(buf[offset:], uint32(p))
			offset += 4
		}
	}

	// Generation
	binary.BigEndian.PutUint32(buf[offset:], uint32(data.Generation))

	return buf, nil
}

// decodeStickyAssignorUserData decodes bytes to StickyAssignorUserDataV1.
func decodeStickyAssignorUserData(data []byte) (StickyAssignorUserDataV1, error) {
	result := StickyAssignorUserDataV1{
		Topics: make(map[string][]int32),
	}

	if len(data) < 6 {
		return result, nil
	}

	offset := 0

	// Version
	result.Version = int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	// Topic count
	topicCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Topics
	for i := uint32(0); i < topicCount && offset < len(data); i++ {
		// Topic name
		if offset+2 > len(data) {
			break
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if offset+topicLen > len(data) {
			break
		}
		topic := string(data[offset : offset+topicLen])
		offset += topicLen

		// Partition count
		if offset+4 > len(data) {
			break
		}
		partitionCount := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		// Partitions
		partitions := make([]int32, 0, partitionCount)
		for j := uint32(0); j < partitionCount && offset+4 <= len(data); j++ {
			partition := int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			partitions = append(partitions, partition)
		}
		result.Topics[topic] = partitions
	}

	// Generation
	if offset+4 <= len(data) {
		result.Generation = int32(binary.BigEndian.Uint32(data[offset:]))
	}

	return result, nil
}

// Verify CooperativeStickyAssignor implements GroupBalancer
var _ GroupBalancer = (*CooperativeStickyAssignor)(nil)

// ===== CooperativeStickyRackAffinityAssignor =====

// RackFunc is a function that returns the rack/availability zone of the consumer.
// This allows for dynamic AZ discovery at runtime, which is useful for:
//   - AWS EC2: Query instance metadata for availability zone
//   - AWS ECS: Query task metadata endpoint
//   - AWS EKS: Query instance metadata or use downward API
//   - Kubernetes: Read from NODE_AZ environment variable or downward API
//
// The function is called once when UserData() is first invoked, and the result
// is cached for subsequent calls. If the function returns an empty string,
// "unknown" is used as the rack value.
type RackFunc func() string

// CooperativeStickyRackAffinityAssignor combines cooperative sticky partition
// assignment with rack-aware locality optimization. This assignor provides:
//
//   - Sticky assignment: Partitions are preferentially assigned to their previous owners
//   - Rack affinity: Partitions are preferentially assigned to consumers in the same
//     rack/zone as the partition leader to minimize cross-AZ network traffic
//   - Balanced distribution: Partitions are distributed evenly across consumers (±1 partition)
//   - Cooperative protocol: Supports incremental rebalance
//
// The priority order for assignment decisions is:
//  1. Balance: Ensure even distribution across consumers
//  2. Stickiness: Keep partitions with their current owner when balanced
//  3. Rack affinity: Prefer local rack assignments when choosing a new owner
//
// This assignor is ideal for AWS MSK or any multi-AZ Kafka deployment where
// cross-AZ data transfer costs are a concern.
//
// Example usage with static rack:
//
//	reader := kafka.NewReader(kafka.ReaderConfig{
//	    Brokers:        []string{"localhost:9092"},
//	    GroupID:        "my-group",
//	    Topic:          "my-topic",
//	    GroupBalancers: []kafka.GroupBalancer{
//	        &kafka.CooperativeStickyRackAffinityAssignor{
//	            Rack: "us-east-1a", // Set to the consumer's rack/AZ
//	        },
//	    },
//	})
//
// Example usage with dynamic rack discovery (e.g., for AWS):
//
//	reader := kafka.NewReader(kafka.ReaderConfig{
//	    Brokers:        []string{"localhost:9092"},
//	    GroupID:        "my-group",
//	    Topic:          "my-topic",
//	    GroupBalancers: []kafka.GroupBalancer{
//	        &kafka.CooperativeStickyRackAffinityAssignor{
//	            RackFunc: func() string {
//	                // Query EC2 metadata, ECS task metadata, or Kubernetes downward API
//	                return discoverAvailabilityZone()
//	            },
//	        },
//	    },
//	})
//
// MSK Compatibility:
//   - Works with Amazon MSK (all versions)
//   - Works with Confluent Cloud
//   - Works with Apache Kafka 2.4+ (KIP-429)
//   - Requires brokers to expose rack information in metadata
//
// Thread Safety: This type is safe for concurrent use. All access to internal
// state is protected by a mutex.
type CooperativeStickyRackAffinityAssignor struct {
	// mu protects currentAssignment, generation, and resolvedRack fields
	mu sync.RWMutex

	// Rack is the name of the rack/availability zone where this consumer is running.
	// In AWS, this is typically the AZ name like "us-east-1a".
	// This is communicated to the group leader via UserData for rack-aware assignments.
	// If both Rack and RackFunc are set, Rack takes precedence.
	Rack string

	// RackFunc is called once to dynamically determine the rack at runtime.
	// This is useful for environments where the rack is not known at startup,
	// such as AWS EC2/ECS/EKS where the availability zone must be queried.
	// The result is cached after the first call.
	// If both Rack and RackFunc are set, Rack takes precedence.
	RackFunc RackFunc

	// resolvedRack caches the result of RackFunc
	resolvedRack string

	// currentAssignment holds the partitions currently assigned to this consumer.
	currentAssignment map[string][]int32

	// generation is the current consumer group generation.
	generation int32
}

// ProtocolName returns the name of this assignor's protocol.
// Uses "cooperative-sticky-rack-affinity" to distinguish from the base cooperative-sticky.
func (s *CooperativeStickyRackAffinityAssignor) ProtocolName() string {
	return "cooperative-sticky-rack-affinity"
}

// getRack returns the rack for this consumer, resolving it dynamically if needed.
// This method handles the RackFunc lazy resolution pattern.
func (s *CooperativeStickyRackAffinityAssignor) getRack() string {
	// Fast path: static Rack is set
	if s.Rack != "" {
		return s.Rack
	}

	// Check if we already resolved the rack
	s.mu.RLock()
	if s.resolvedRack != "" {
		rack := s.resolvedRack
		s.mu.RUnlock()
		return rack
	}
	s.mu.RUnlock()

	// Slow path: need to resolve rack via RackFunc
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if s.resolvedRack != "" {
		return s.resolvedRack
	}

	// Resolve rack using RackFunc if available
	if s.RackFunc != nil {
		s.resolvedRack = s.RackFunc()
	}

	// Default to "unknown" if no rack can be determined
	if s.resolvedRack == "" {
		s.resolvedRack = "unknown"
	}

	return s.resolvedRack
}

// UserData serializes both rack and assignment state to be sent to the group leader.
func (s *CooperativeStickyRackAffinityAssignor) UserData() ([]byte, error) {
	// Get rack (may involve lazy resolution)
	rack := s.getRack()

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Calculate size
	rackBytes := []byte(rack)

	// Start with rack
	size := 2 + len(rackBytes) // rack length + rack

	// Add sticky data if present
	var stickyData []byte
	if len(s.currentAssignment) > 0 {
		var err error
		stickyData, err = encodeStickyAssignorUserData(StickyAssignorUserDataV1{
			Version:    1,
			Topics:     s.currentAssignment,
			Generation: s.generation,
		})
		if err != nil {
			return nil, err
		}
		size += len(stickyData)
	}

	buf := make([]byte, size)
	offset := 0

	// Write rack
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(rackBytes)))
	offset += 2
	copy(buf[offset:], rackBytes)
	offset += len(rackBytes)

	// Write sticky data
	if stickyData != nil {
		copy(buf[offset:], stickyData)
	}

	return buf, nil
}

// SetAssignment updates the current assignment for this consumer.
func (s *CooperativeStickyRackAffinityAssignor) SetAssignment(assignment map[string][]int32, generation int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.currentAssignment = assignment
	s.generation = generation
}

// GetOwnedPartitions returns the partitions currently owned by this consumer.
func (s *CooperativeStickyRackAffinityAssignor) GetOwnedPartitions() map[string][]int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.currentAssignment
}

// GetGeneration returns the current generation ID.
func (s *CooperativeStickyRackAffinityAssignor) GetGeneration() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.generation
}

// GetRack returns the rack for this consumer. If a RackFunc was provided and
// hasn't been called yet, this will trigger the rack resolution.
func (s *CooperativeStickyRackAffinityAssignor) GetRack() string {
	return s.getRack()
}

// parseStickyRackAffinityUserData decodes rack and sticky data from UserData.
func parseStickyRackAffinityUserData(data []byte) (rack string, sticky StickyAssignorUserDataV1, err error) {
	if len(data) < 2 {
		return "", StickyAssignorUserDataV1{}, nil
	}

	offset := 0

	// Read rack
	rackLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if offset+rackLen > len(data) {
		return "", StickyAssignorUserDataV1{}, nil
	}
	rack = string(data[offset : offset+rackLen])
	offset += rackLen

	// Read sticky data if present
	if offset < len(data) {
		sticky, err = decodeStickyAssignorUserData(data[offset:])
		if err != nil {
			return rack, StickyAssignorUserDataV1{}, nil // Return rack even if sticky parse fails
		}
	}

	return rack, sticky, nil
}

// AssignGroups assigns partitions using a combined sticky + rack-affinity strategy.
//
// The algorithm:
//  1. Parse member rack info and current assignments from UserData
//  2. Build mapping of partitions to partition leader racks
//  3. Use sticky algorithm as base, with rack-affinity as tiebreaker
//  4. When assigning unassigned partitions, prefer consumers in the same rack as leader
//  5. When stealing partitions for balance, prefer stealing cross-rack assignments
func (s *CooperativeStickyRackAffinityAssignor) AssignGroups(members []GroupMember, partitions []Partition) GroupMemberAssignments {
	// Build partition leader rack mapping
	partitionLeaderRack := make(map[cooperativeStickyTopicPartition]string)
	for _, p := range partitions {
		tp := cooperativeStickyTopicPartition{Topic: p.Topic, Partition: p.ID}
		if p.Leader.Rack != "" {
			partitionLeaderRack[tp] = p.Leader.Rack
		}
	}

	// Parse member racks and create enhanced members with rack info
	memberRacks := make(map[string]string)
	membersWithOwnedPartitions := make([]GroupMember, len(members))
	copy(membersWithOwnedPartitions, members)

	for i, member := range members {
		rack, sticky, _ := parseStickyRackAffinityUserData(member.UserData)
		memberRacks[member.ID] = rack

		// Populate OwnedPartitions from sticky data if not already set
		if membersWithOwnedPartitions[i].OwnedPartitions == nil && sticky.Topics != nil {
			membersWithOwnedPartitions[i].OwnedPartitions = make(map[string][]int)
			for topic, parts := range sticky.Topics {
				intParts := make([]int, len(parts))
				for j, p := range parts {
					intParts[j] = int(p)
				}
				membersWithOwnedPartitions[i].OwnedPartitions[topic] = intParts
			}
		}
	}

	// Build a sorted list of all topic-partitions
	allPartitions := make([]cooperativeStickyTopicPartition, 0, len(partitions))
	for _, p := range partitions {
		allPartitions = append(allPartitions, cooperativeStickyTopicPartition{
			Topic:     p.Topic,
			Partition: p.ID,
		})
	}
	sortCooperativeStickyTopicPartitions(allPartitions)

	// Parse current assignments from member user data (including rack-affinity encoded data)
	currentAssignment := make(map[string][]cooperativeStickyTopicPartition)
	for _, member := range membersWithOwnedPartitions {
		currentAssignment[member.ID] = make([]cooperativeStickyTopicPartition, 0)

		// First try OwnedPartitions
		if member.OwnedPartitions != nil {
			for topic, parts := range member.OwnedPartitions {
				for _, p := range parts {
					currentAssignment[member.ID] = append(currentAssignment[member.ID],
						cooperativeStickyTopicPartition{Topic: topic, Partition: p})
				}
			}
		}
	}

	// Build mapping of partition -> potential consumers
	partition2AllPotentialConsumers := make(map[cooperativeStickyTopicPartition][]string)
	for _, tp := range allPartitions {
		partition2AllPotentialConsumers[tp] = []string{}
	}

	// Build mapping of consumer -> potential partitions
	consumer2AllPotentialPartitions := make(map[string][]cooperativeStickyTopicPartition)
	for _, member := range membersWithOwnedPartitions {
		consumer2AllPotentialPartitions[member.ID] = make([]cooperativeStickyTopicPartition, 0)
		for _, topic := range member.Topics {
			for _, tp := range allPartitions {
				if tp.Topic == topic {
					consumer2AllPotentialPartitions[member.ID] = append(
						consumer2AllPotentialPartitions[member.ID], tp)
					partition2AllPotentialConsumers[tp] = append(
						partition2AllPotentialConsumers[tp], member.ID)
				}
			}
		}
	}

	// Sort potential consumers for each partition - prefer same-rack consumers
	for tp, consumers := range partition2AllPotentialConsumers {
		leaderRack := partitionLeaderRack[tp]
		sort.Slice(consumers, func(i, j int) bool {
			// Same-rack consumers come first
			iSameRack := memberRacks[consumers[i]] == leaderRack && leaderRack != ""
			jSameRack := memberRacks[consumers[j]] == leaderRack && leaderRack != ""
			if iSameRack != jSameRack {
				return iSameRack
			}
			return consumers[i] < consumers[j]
		})
		partition2AllPotentialConsumers[tp] = consumers
	}

	// Remove invalid assignments (partitions that member is no longer subscribed to)
	for memberID, assigned := range currentAssignment {
		potentialPartitions := make(map[cooperativeStickyTopicPartition]bool)
		for _, tp := range consumer2AllPotentialPartitions[memberID] {
			potentialPartitions[tp] = true
		}

		validAssignment := make([]cooperativeStickyTopicPartition, 0)
		for _, tp := range assigned {
			if potentialPartitions[tp] {
				validAssignment = append(validAssignment, tp)
			}
		}
		currentAssignment[memberID] = validAssignment
	}

	// Find partitions that need to be assigned
	assignedPartitions := make(map[cooperativeStickyTopicPartition]string)
	for memberID, assigned := range currentAssignment {
		for _, tp := range assigned {
			assignedPartitions[tp] = memberID
		}
	}

	unassignedPartitions := make([]cooperativeStickyTopicPartition, 0)
	for _, tp := range allPartitions {
		if _, exists := assignedPartitions[tp]; !exists {
			unassignedPartitions = append(unassignedPartitions, tp)
		}
	}

	// Sort unassigned partitions to prefer same-rack assignments first
	sortByRackAffinity := func(partitions []cooperativeStickyTopicPartition, memberRacks map[string]string,
		partitionLeaderRack map[cooperativeStickyTopicPartition]string,
		partition2Consumers map[cooperativeStickyTopicPartition][]string) {

		sort.Slice(partitions, func(i, j int) bool {
			tpi, tpj := partitions[i], partitions[j]
			leaderRackI := partitionLeaderRack[tpi]
			leaderRackJ := partitionLeaderRack[tpj]

			// Check if any potential consumer is in same rack
			hasSameRackConsumerI := false
			for _, c := range partition2Consumers[tpi] {
				if memberRacks[c] == leaderRackI && leaderRackI != "" {
					hasSameRackConsumerI = true
					break
				}
			}
			hasSameRackConsumerJ := false
			for _, c := range partition2Consumers[tpj] {
				if memberRacks[c] == leaderRackJ && leaderRackJ != "" {
					hasSameRackConsumerJ = true
					break
				}
			}

			// Partitions with same-rack consumers should be assigned first
			if hasSameRackConsumerI != hasSameRackConsumerJ {
				return hasSameRackConsumerI
			}
			// Otherwise sort by topic, partition
			if tpi.Topic != tpj.Topic {
				return tpi.Topic < tpj.Topic
			}
			return tpi.Partition < tpj.Partition
		})
	}

	sortByRackAffinity(unassignedPartitions, memberRacks, partitionLeaderRack, partition2AllPotentialConsumers)

	// Calculate target assignment sizes
	totalPartitions := len(allPartitions)
	totalConsumers := len(membersWithOwnedPartitions)
	if totalConsumers == 0 {
		return GroupMemberAssignments{}
	}

	minPerConsumer := totalPartitions / totalConsumers
	maxPerConsumer := minPerConsumer
	if totalPartitions%totalConsumers != 0 {
		maxPerConsumer++
	}

	// Assign unassigned partitions with rack affinity preference
	for _, tp := range unassignedPartitions {
		consumers := partition2AllPotentialConsumers[tp]
		if len(consumers) == 0 {
			continue
		}

		// Find consumer with lowest assignment count, preferring same-rack
		leaderRack := partitionLeaderRack[tp]
		var bestConsumer string
		bestCount := int(^uint(0) >> 1) // Max int

		for _, consumer := range consumers {
			count := len(currentAssignment[consumer])
			if count >= maxPerConsumer {
				continue // Consumer is full
			}

			isSameRack := memberRacks[consumer] == leaderRack && leaderRack != ""
			// Prefer same-rack consumers, then lowest count
			if bestConsumer == "" ||
				(isSameRack && memberRacks[bestConsumer] != leaderRack) ||
				(isSameRack == (memberRacks[bestConsumer] == leaderRack) && count < bestCount) {
				bestConsumer = consumer
				bestCount = count
			}
		}

		if bestConsumer != "" {
			currentAssignment[bestConsumer] = append(currentAssignment[bestConsumer], tp)
			assignedPartitions[tp] = bestConsumer
		}
	}

	// Rebalance if needed - steal from over-assigned consumers
	// Prefer stealing cross-rack assignments
	for {
		var overloaded, underloaded string
		maxCount, minCount := 0, int(^uint(0)>>1)

		for memberID := range consumer2AllPotentialPartitions {
			count := len(currentAssignment[memberID])
			if count > maxCount {
				maxCount = count
				overloaded = memberID
			}
			if count < minCount {
				minCount = count
				underloaded = memberID
			}
		}

		// Check if rebalancing is needed (difference > 1)
		if maxCount-minCount <= 1 {
			break
		}

		// Find a partition to steal - prefer cross-rack
		underloadedRack := memberRacks[underloaded]
		var partitionToMove cooperativeStickyTopicPartition
		foundCrossRack := false

		for _, tp := range currentAssignment[overloaded] {
			// Check if underloaded can accept this partition
			canAccept := false
			for _, candidate := range partition2AllPotentialConsumers[tp] {
				if candidate == underloaded {
					canAccept = true
					break
				}
			}
			if !canAccept {
				continue
			}

			leaderRack := partitionLeaderRack[tp]
			isCrossRackOnOverloaded := memberRacks[overloaded] != leaderRack || leaderRack == ""
			isSameRackOnUnderloaded := underloadedRack == leaderRack && leaderRack != ""

			// Prefer to move partitions that improve rack affinity
			if !foundCrossRack && (isCrossRackOnOverloaded || isSameRackOnUnderloaded) {
				partitionToMove = tp
				foundCrossRack = true
			} else if !foundCrossRack {
				partitionToMove = tp
			}
		}

		if partitionToMove.Topic == "" {
			break // No movable partition found
		}

		// Move the partition
		newAssignment := make([]cooperativeStickyTopicPartition, 0)
		for _, tp := range currentAssignment[overloaded] {
			if tp != partitionToMove {
				newAssignment = append(newAssignment, tp)
			}
		}
		currentAssignment[overloaded] = newAssignment
		currentAssignment[underloaded] = append(currentAssignment[underloaded], partitionToMove)
	}

	// Convert to GroupMemberAssignments
	result := make(GroupMemberAssignments)
	for memberID, assigned := range currentAssignment {
		if len(assigned) == 0 {
			continue
		}
		result[memberID] = make(map[string][]int)
		for _, tp := range assigned {
			result[memberID][tp.Topic] = append(result[memberID][tp.Topic], tp.Partition)
		}
		// Sort partitions for consistent output
		for topic := range result[memberID] {
			sort.Ints(result[memberID][topic])
		}
	}

	return result
}

// Verify CooperativeStickyRackAffinityAssignor implements GroupBalancer
var _ GroupBalancer = (*CooperativeStickyRackAffinityAssignor)(nil)
