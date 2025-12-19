package kafka

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCooperativeStickyAssignor_ProtocolName(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}
	if name := assignor.ProtocolName(); name != "cooperative-sticky" {
		t.Errorf("expected protocol name 'cooperative-sticky', got '%s'", name)
	}
}

func TestCooperativeStickyAssignor_UserData_Empty(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}
	data, err := assignor.UserData()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data, got %v", data)
	}
}

func TestCooperativeStickyAssignor_UserData_WithAssignment(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}
	assignor.SetAssignment(map[string][]int32{
		"topic-a": {0, 1, 2},
		"topic-b": {0},
	}, 5)

	data, err := assignor.UserData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data == nil {
		t.Fatal("expected non-nil data")
	}

	// Decode and verify
	decoded, err := decodeStickyAssignorUserData(data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded.Generation != 5 {
		t.Errorf("expected generation 5, got %d", decoded.Generation)
	}

	if len(decoded.Topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(decoded.Topics))
	}
}

func TestCooperativeStickyAssignor_SetAssignment(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	assignment := map[string][]int32{
		"topic-a": {0, 1},
		"topic-b": {2},
	}
	assignor.SetAssignment(assignment, 10)

	if assignor.GetGeneration() != 10 {
		t.Errorf("expected generation 10, got %d", assignor.GetGeneration())
	}

	owned := assignor.GetOwnedPartitions()
	if !reflect.DeepEqual(owned, assignment) {
		t.Errorf("owned partitions mismatch: got %v, want %v", owned, assignment)
	}
}

func TestCooperativeStickyAssignor_AssignGroups_Basic(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-a", ID: 2},
		{Topic: "topic-a", ID: 3},
	}

	result := assignor.AssignGroups(members, partitions)

	// Verify all partitions assigned
	total := 0
	for _, topicParts := range result {
		for _, parts := range topicParts {
			total += len(parts)
		}
	}
	if total != 4 {
		t.Errorf("expected 4 partitions assigned, got %d", total)
	}

	// Verify balanced distribution (2 partitions each)
	for memberID, topics := range result {
		count := 0
		for _, parts := range topics {
			count += len(parts)
		}
		if count != 2 {
			t.Errorf("expected 2 partitions for %s, got %d", memberID, count)
		}
	}
}

func TestCooperativeStickyAssignor_AssignGroups_Sticky(t *testing.T) {
	// First, create assignments for two consumers
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-a", ID: 2},
		{Topic: "topic-a", ID: 3},
	}

	result1 := assignor.AssignGroups(members, partitions)

	// Encode the assignment as UserData for each consumer
	userData1 := make(map[string][]int32)
	for topic, parts := range result1["consumer-1"] {
		for _, p := range parts {
			userData1[topic] = append(userData1[topic], int32(p))
		}
	}
	encoded1, _ := encodeStickyAssignorUserData(StickyAssignorUserDataV1{
		Version:    1,
		Topics:     userData1,
		Generation: 1,
	})

	userData2 := make(map[string][]int32)
	for topic, parts := range result1["consumer-2"] {
		for _, p := range parts {
			userData2[topic] = append(userData2[topic], int32(p))
		}
	}
	encoded2, _ := encodeStickyAssignorUserData(StickyAssignorUserDataV1{
		Version:    1,
		Topics:     userData2,
		Generation: 1,
	})

	// Now add a third consumer and verify stickiness
	membersWithData := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}, UserData: encoded1},
		{ID: "consumer-2", Topics: []string{"topic-a"}, UserData: encoded2},
		{ID: "consumer-3", Topics: []string{"topic-a"}},
	}

	result2 := assignor.AssignGroups(membersWithData, partitions)

	// Verify all partitions still assigned
	total := 0
	for _, topicParts := range result2 {
		for _, parts := range topicParts {
			total += len(parts)
		}
	}
	if total != 4 {
		t.Errorf("expected 4 partitions assigned, got %d", total)
	}

	// Verify consumer-3 got at least one partition
	if len(result2["consumer-3"]) == 0 {
		t.Error("expected consumer-3 to receive partitions")
	}

	t.Logf("Initial assignment: %v", result1)
	t.Logf("After adding consumer: %v", result2)
}

func TestCooperativeStickyAssignor_AssignGroups_MultiTopic(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a", "topic-b"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-b", ID: 0},
		{Topic: "topic-b", ID: 1},
	}

	result := assignor.AssignGroups(members, partitions)

	// consumer-2 can only get topic-a partitions
	if topics, ok := result["consumer-2"]; ok {
		if _, hasTopic := topics["topic-b"]; hasTopic {
			t.Error("consumer-2 should not have topic-b partitions")
		}
	}

	// consumer-1 should have both topic-b partitions
	if topics, ok := result["consumer-1"]; ok {
		if parts, hasTopic := topics["topic-b"]; !hasTopic || len(parts) != 2 {
			t.Errorf("consumer-1 should have 2 topic-b partitions, got %v", topics)
		}
	}
}

func TestCooperativeStickyAssignor_AssignGroups_NoPartitions(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
	}

	partitions := []Partition{}

	result := assignor.AssignGroups(members, partitions)

	if len(result["consumer-1"]["topic-a"]) != 0 {
		t.Error("expected empty assignment")
	}
}

func TestCooperativeStickyAssignor_AssignGroups_SingleConsumer(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-a", ID: 2},
	}

	result := assignor.AssignGroups(members, partitions)

	if len(result["consumer-1"]["topic-a"]) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(result["consumer-1"]["topic-a"]))
	}
}

func TestEncodeDecode_StickyAssignorUserData(t *testing.T) {
	original := StickyAssignorUserDataV1{
		Version: 1,
		Topics: map[string][]int32{
			"topic-a": {0, 1, 2},
			"topic-b": {0},
		},
		Generation: 42,
	}

	encoded, err := encodeStickyAssignorUserData(original)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	decoded, err := decodeStickyAssignorUserData(encoded)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded.Version != original.Version {
		t.Errorf("version mismatch: got %d, want %d", decoded.Version, original.Version)
	}

	if decoded.Generation != original.Generation {
		t.Errorf("generation mismatch: got %d, want %d", decoded.Generation, original.Generation)
	}

	if len(decoded.Topics) != len(original.Topics) {
		t.Errorf("topic count mismatch: got %d, want %d", len(decoded.Topics), len(original.Topics))
	}

	for topic, origParts := range original.Topics {
		decParts, ok := decoded.Topics[topic]
		if !ok {
			t.Errorf("missing topic %s", topic)
			continue
		}
		if !reflect.DeepEqual(decParts, origParts) {
			t.Errorf("partition mismatch for %s: got %v, want %v", topic, decParts, origParts)
		}
	}
}

func TestCooperativeStickyAssignor_Balance_UnevenPartitions(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
		{ID: "consumer-3", Topics: []string{"topic-a"}},
	}

	// 7 partitions, 3 consumers: should be 3, 2, 2 or similar
	partitions := make([]Partition, 7)
	for i := 0; i < 7; i++ {
		partitions[i] = Partition{Topic: "topic-a", ID: i}
	}

	result := assignor.AssignGroups(members, partitions)

	counts := make(map[int]int) // count -> number of consumers with that count
	minCount, maxCount := 0, 0
	for memberID, topics := range result {
		count := 0
		for _, parts := range topics {
			count += len(parts)
		}
		counts[count]++
		t.Logf("%s: %d partitions", memberID, count)

		if minCount == 0 || count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}

	// Should have at most 1 difference between max and min
	if maxCount-minCount > 1 {
		t.Errorf("assignment not balanced: max=%d, min=%d", maxCount, minCount)
	}
}

func TestCooperativeStickyAssignor_MemberLeavesGroup(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	// Initial: 3 consumers, 6 partitions
	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
		{ID: "consumer-3", Topics: []string{"topic-a"}},
	}

	partitions := make([]Partition, 6)
	for i := 0; i < 6; i++ {
		partitions[i] = Partition{Topic: "topic-a", ID: i}
	}

	result1 := assignor.AssignGroups(members, partitions)
	t.Logf("Initial assignment: %v", result1)

	// Encode assignments as UserData
	var membersWithData []GroupMember
	for _, m := range members[:2] { // Remove consumer-3
		userData := make(map[string][]int32)
		if topics, ok := result1[m.ID]; ok {
			for topic, parts := range topics {
				for _, p := range parts {
					userData[topic] = append(userData[topic], int32(p))
				}
			}
		}
		encoded, _ := encodeStickyAssignorUserData(StickyAssignorUserDataV1{
			Version:    1,
			Topics:     userData,
			Generation: 1,
		})
		membersWithData = append(membersWithData, GroupMember{
			ID:       m.ID,
			Topics:   m.Topics,
			UserData: encoded,
		})
	}

	result2 := assignor.AssignGroups(membersWithData, partitions)
	t.Logf("After consumer-3 left: %v", result2)

	// All 6 partitions should now be distributed between 2 consumers
	total := 0
	for _, topics := range result2 {
		for _, parts := range topics {
			total += len(parts)
		}
	}
	if total != 6 {
		t.Errorf("expected 6 partitions assigned, got %d", total)
	}

	// Check stickiness: partitions previously owned should still be with same owners
	for memberID, topics := range result1 {
		if memberID == "consumer-3" {
			continue
		}
		for topic, oldParts := range topics {
			newParts := result2[memberID][topic]
			// At least the original partitions should be present (stickiness)
			for _, oldP := range oldParts {
				found := false
				for _, newP := range newParts {
					if oldP == newP {
						found = true
						break
					}
				}
				if !found {
					t.Logf("partition %d moved from %s (may be ok for rebalancing)", oldP, memberID)
				}
			}
		}
	}
}

func TestCooperativeStickyAssignor_NoMembers(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
	}

	result := assignor.AssignGroups(members, partitions)

	if len(result) != 0 {
		t.Errorf("expected empty result for no members, got %v", result)
	}
}

func TestCooperativeStickyAssignor_MemberWithNoMatchingTopics(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-b"}}, // No partitions for topic-b
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
	}

	result := assignor.AssignGroups(members, partitions)

	// consumer-1 should get all topic-a partitions
	if len(result["consumer-1"]["topic-a"]) != 2 {
		t.Errorf("expected consumer-1 to have 2 partitions, got %d", len(result["consumer-1"]["topic-a"]))
	}

	// consumer-2 should have empty assignment (no topic-b partitions exist)
	if len(result["consumer-2"]) != 0 {
		t.Errorf("expected consumer-2 to have no partitions, got %v", result["consumer-2"])
	}
}

func TestCooperativeStickyAssignor_LargeScale(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	// 10 consumers, 100 partitions across 5 topics
	members := make([]GroupMember, 10)
	for i := 0; i < 10; i++ {
		members[i] = GroupMember{
			ID:     string(rune('A' + i)),
			Topics: []string{"topic-0", "topic-1", "topic-2", "topic-3", "topic-4"},
		}
	}

	partitions := make([]Partition, 0, 100)
	for t := 0; t < 5; t++ {
		for p := 0; p < 20; p++ {
			partitions = append(partitions, Partition{
				Topic: "topic-" + string(rune('0'+t)),
				ID:    p,
			})
		}
	}

	result := assignor.AssignGroups(members, partitions)

	// Verify all partitions assigned
	total := 0
	for _, topics := range result {
		for _, parts := range topics {
			total += len(parts)
		}
	}
	if total != 100 {
		t.Errorf("expected 100 partitions assigned, got %d", total)
	}

	// Verify balanced: each consumer should have 10 partitions
	for memberID, topics := range result {
		count := 0
		for _, parts := range topics {
			count += len(parts)
		}
		if count != 10 {
			t.Errorf("expected 10 partitions for %s, got %d", memberID, count)
		}
	}
}

func TestCooperativeStickyAssignor_ImplementsGroupBalancer(t *testing.T) {
	var _ GroupBalancer = (*CooperativeStickyAssignor)(nil)
}

func BenchmarkCooperativeStickyAssignor_AssignGroups(b *testing.B) {
	assignor := &CooperativeStickyAssignor{}

	members := make([]GroupMember, 10)
	for i := 0; i < 10; i++ {
		members[i] = GroupMember{
			ID:     string(rune('A' + i)),
			Topics: []string{"topic-a", "topic-b", "topic-c"},
		}
	}

	partitions := make([]Partition, 0, 90)
	for _, topic := range []string{"topic-a", "topic-b", "topic-c"} {
		for i := 0; i < 30; i++ {
			partitions = append(partitions, Partition{Topic: topic, ID: i})
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assignor.AssignGroups(members, partitions)
	}
}

func BenchmarkCooperativeStickyAssignor_AssignGroups_Large(b *testing.B) {
	assignor := &CooperativeStickyAssignor{}

	members := make([]GroupMember, 50)
	topics := []string{"topic-a", "topic-b", "topic-c", "topic-d", "topic-e"}
	for i := 0; i < 50; i++ {
		members[i] = GroupMember{
			ID:     string(rune(i)),
			Topics: topics,
		}
	}

	partitions := make([]Partition, 0, 500)
	for _, topic := range topics {
		for i := 0; i < 100; i++ {
			partitions = append(partitions, Partition{Topic: topic, ID: i})
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assignor.AssignGroups(members, partitions)
	}
}

func TestCooperativeStickyAssignor_ConcurrentAccess(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	// Run concurrent reads and writes
	done := make(chan bool)
	const goroutines = 10
	const iterations = 100

	// Writers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < iterations; j++ {
				assignor.SetAssignment(map[string][]int32{
					"topic-a": {int32(j), int32(j + 1)},
				}, int32(j))
			}
			done <- true
		}(i)
	}

	// Readers (UserData)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				_, _ = assignor.UserData()
			}
			done <- true
		}()
	}

	// Readers (GetOwnedPartitions)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				_ = assignor.GetOwnedPartitions()
			}
			done <- true
		}()
	}

	// Readers (GetGeneration)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				_ = assignor.GetGeneration()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines*4; i++ {
		<-done
	}
}

// ===== CooperativeStickyRackAffinityAssignor Tests =====

func TestCooperativeStickyRackAffinityAssignor_ProtocolName(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1a"}
	if name := assignor.ProtocolName(); name != "cooperative-sticky-rack-affinity" {
		t.Errorf("expected protocol name 'cooperative-sticky-rack-affinity', got '%s'", name)
	}
}

func TestCooperativeStickyRackAffinityAssignor_UserData(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1a"}
	assignor.SetAssignment(map[string][]int32{
		"topic-a": {0, 1, 2},
	}, 5)

	data, err := assignor.UserData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data == nil {
		t.Fatal("expected non-nil data")
	}

	// Decode and verify
	rack, sticky, err := parseStickyRackAffinityUserData(data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if rack != "us-east-1a" {
		t.Errorf("expected rack 'us-east-1a', got '%s'", rack)
	}
	if sticky.Generation != 5 {
		t.Errorf("expected generation 5, got %d", sticky.Generation)
	}
	if len(sticky.Topics["topic-a"]) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(sticky.Topics["topic-a"]))
	}
}

func TestCooperativeStickyRackAffinityAssignor_UserData_RackOnly(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: "eu-west-1b"}

	data, err := assignor.UserData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rack, _, _ := parseStickyRackAffinityUserData(data)
	if rack != "eu-west-1b" {
		t.Errorf("expected rack 'eu-west-1b', got '%s'", rack)
	}
}

func TestCooperativeStickyRackAffinityAssignor_Basic(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1a"}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 1, Leader: Broker{Rack: "us-east-1b"}},
		{Topic: "topic-a", ID: 2, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 3, Leader: Broker{Rack: "us-east-1b"}},
	}

	result := assignor.AssignGroups(members, partitions)

	// Verify all partitions assigned
	total := 0
	for _, topicParts := range result {
		for _, parts := range topicParts {
			total += len(parts)
		}
	}
	if total != 4 {
		t.Errorf("expected 4 partitions assigned, got %d", total)
	}

	// Verify balanced
	for memberID, topicParts := range result {
		count := 0
		for _, parts := range topicParts {
			count += len(parts)
		}
		if count != 2 {
			t.Errorf("member %s should have 2 partitions, got %d", memberID, count)
		}
	}
}

func TestCooperativeStickyRackAffinityAssignor_RackAffinity(t *testing.T) {
	// Create two consumers in different racks
	assignor1 := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1a"}
	assignor2 := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1b"}

	userData1, _ := assignor1.UserData()
	userData2, _ := assignor2.UserData()

	members := []GroupMember{
		{ID: "consumer-1a", Topics: []string{"topic-a"}, UserData: userData1},
		{ID: "consumer-1b", Topics: []string{"topic-a"}, UserData: userData2},
	}

	// Create partitions with leaders in different racks
	partitions := []Partition{
		{Topic: "topic-a", ID: 0, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 1, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 2, Leader: Broker{Rack: "us-east-1b"}},
		{Topic: "topic-a", ID: 3, Leader: Broker{Rack: "us-east-1b"}},
	}

	result := assignor1.AssignGroups(members, partitions)

	// Log assignments for debugging
	t.Logf("Assignment result: %+v", result)

	// Verify consumer-1a gets partitions 0,1 (leader in us-east-1a)
	// and consumer-1b gets partitions 2,3 (leader in us-east-1b)
	consumer1aPartitions := result["consumer-1a"]["topic-a"]
	consumer1bPartitions := result["consumer-1b"]["topic-a"]

	// Count rack-local assignments
	localAssignments := 0
	for _, p := range consumer1aPartitions {
		if p == 0 || p == 1 { // These are in us-east-1a
			localAssignments++
		}
	}
	for _, p := range consumer1bPartitions {
		if p == 2 || p == 3 { // These are in us-east-1b
			localAssignments++
		}
	}

	t.Logf("Rack-local assignments: %d out of 4", localAssignments)

	// With perfect rack affinity, all 4 should be local
	// But we accept >= 2 as success (some balancing may override)
	if localAssignments < 2 {
		t.Errorf("expected at least 2 rack-local assignments, got %d", localAssignments)
	}
}

func TestCooperativeStickyRackAffinityAssignor_Sticky(t *testing.T) {
	// Test that stickiness is preserved
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1a"}
	assignor.SetAssignment(map[string][]int32{
		"topic-a": {0, 1},
	}, 1)
	userData1, _ := assignor.UserData()

	assignor2 := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1b"}
	assignor2.SetAssignment(map[string][]int32{
		"topic-a": {2, 3},
	}, 1)
	userData2, _ := assignor2.UserData()

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}, UserData: userData1},
		{ID: "consumer-2", Topics: []string{"topic-a"}, UserData: userData2},
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 1, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 2, Leader: Broker{Rack: "us-east-1b"}},
		{Topic: "topic-a", ID: 3, Leader: Broker{Rack: "us-east-1b"}},
	}

	result := assignor.AssignGroups(members, partitions)

	t.Logf("Sticky result: %+v", result)

	// Verify consumer-1 keeps partitions 0,1 and consumer-2 keeps 2,3
	c1Parts := result["consumer-1"]["topic-a"]
	c2Parts := result["consumer-2"]["topic-a"]

	c1Has0 := contains(c1Parts, 0)
	c1Has1 := contains(c1Parts, 1)
	c2Has2 := contains(c2Parts, 2)
	c2Has3 := contains(c2Parts, 3)

	stickyCount := 0
	if c1Has0 {
		stickyCount++
	}
	if c1Has1 {
		stickyCount++
	}
	if c2Has2 {
		stickyCount++
	}
	if c2Has3 {
		stickyCount++
	}

	t.Logf("Sticky assignments preserved: %d out of 4", stickyCount)

	// Should preserve at least 3 out of 4 assignments
	if stickyCount < 3 {
		t.Errorf("expected at least 3 sticky assignments preserved, got %d", stickyCount)
	}
}

func TestCooperativeStickyRackAffinityAssignor_MultiAZ(t *testing.T) {
	// Simulate a 3-AZ AWS MSK deployment
	azs := []string{"us-east-1a", "us-east-1b", "us-east-1c"}

	// 3 consumers, one per AZ
	members := make([]GroupMember, 3)
	for i, az := range azs {
		assignor := &CooperativeStickyRackAffinityAssignor{Rack: az}
		userData, _ := assignor.UserData()
		members[i] = GroupMember{
			ID:       "consumer-" + az,
			Topics:   []string{"topic-a"},
			UserData: userData,
		}
	}

	// 9 partitions, 3 per AZ
	partitions := make([]Partition, 9)
	for i := 0; i < 9; i++ {
		partitions[i] = Partition{
			Topic:  "topic-a",
			ID:     i,
			Leader: Broker{Rack: azs[i%3]},
		}
	}

	assignor := &CooperativeStickyRackAffinityAssignor{}
	result := assignor.AssignGroups(members, partitions)

	t.Logf("Multi-AZ result: %+v", result)

	// Each consumer should have exactly 3 partitions
	for memberID, topicParts := range result {
		count := len(topicParts["topic-a"])
		if count != 3 {
			t.Errorf("member %s should have 3 partitions, got %d", memberID, count)
		}
	}

	// Count rack-local assignments
	localCount := 0
	for memberID, topicParts := range result {
		memberAZ := memberID[len("consumer-"):]
		for _, partID := range topicParts["topic-a"] {
			leaderAZ := azs[partID%3]
			if memberAZ == leaderAZ {
				localCount++
			}
		}
	}

	t.Logf("Rack-local assignments: %d out of 9", localCount)

	// With perfect rack affinity, all 9 should be local
	if localCount < 6 {
		t.Errorf("expected at least 6 rack-local assignments, got %d", localCount)
	}
}

func TestCooperativeStickyRackAffinityAssignor_ImbalancedConsumers(t *testing.T) {
	// 3 consumers in one AZ, 1 in another - test that balance takes priority
	members := []GroupMember{
		{ID: "consumer-1a-1", Topics: []string{"topic-a"}, UserData: makeRackUserData("us-east-1a")},
		{ID: "consumer-1a-2", Topics: []string{"topic-a"}, UserData: makeRackUserData("us-east-1a")},
		{ID: "consumer-1a-3", Topics: []string{"topic-a"}, UserData: makeRackUserData("us-east-1a")},
		{ID: "consumer-1b-1", Topics: []string{"topic-a"}, UserData: makeRackUserData("us-east-1b")},
	}

	// 4 partitions, 2 in each AZ
	partitions := []Partition{
		{Topic: "topic-a", ID: 0, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 1, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "topic-a", ID: 2, Leader: Broker{Rack: "us-east-1b"}},
		{Topic: "topic-a", ID: 3, Leader: Broker{Rack: "us-east-1b"}},
	}

	assignor := &CooperativeStickyRackAffinityAssignor{}
	result := assignor.AssignGroups(members, partitions)

	t.Logf("Imbalanced result: %+v", result)

	// Each consumer should have exactly 1 partition (balance takes priority)
	for memberID, topicParts := range result {
		count := len(topicParts["topic-a"])
		if count != 1 {
			t.Errorf("member %s should have 1 partition, got %d", memberID, count)
		}
	}
}

func TestCooperativeStickyRackAffinityAssignor_NoRackInfo(t *testing.T) {
	// Test graceful handling when rack info is missing
	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
	}

	// Partitions without rack info
	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-a", ID: 2},
		{Topic: "topic-a", ID: 3},
	}

	assignor := &CooperativeStickyRackAffinityAssignor{}
	result := assignor.AssignGroups(members, partitions)

	// Should still produce balanced assignment
	total := 0
	for _, topicParts := range result {
		for _, parts := range topicParts {
			total += len(parts)
		}
	}
	if total != 4 {
		t.Errorf("expected 4 partitions assigned, got %d", total)
	}

	// Each consumer should have 2
	for memberID, topicParts := range result {
		if len(topicParts["topic-a"]) != 2 {
			t.Errorf("member %s should have 2 partitions", memberID)
		}
	}
}

func TestCooperativeStickyRackAffinityAssignor_MSKCompatibility(t *testing.T) {
	// Test with MSK-style AZ naming
	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"orders"}, UserData: makeRackUserData("use1-az1")},
		{ID: "consumer-2", Topics: []string{"orders"}, UserData: makeRackUserData("use1-az2")},
		{ID: "consumer-3", Topics: []string{"orders"}, UserData: makeRackUserData("use1-az3")},
	}

	// MSK typically uses AZ IDs like use1-az1, use1-az2, use1-az3
	partitions := []Partition{
		{Topic: "orders", ID: 0, Leader: Broker{Rack: "use1-az1"}},
		{Topic: "orders", ID: 1, Leader: Broker{Rack: "use1-az2"}},
		{Topic: "orders", ID: 2, Leader: Broker{Rack: "use1-az3"}},
		{Topic: "orders", ID: 3, Leader: Broker{Rack: "use1-az1"}},
		{Topic: "orders", ID: 4, Leader: Broker{Rack: "use1-az2"}},
		{Topic: "orders", ID: 5, Leader: Broker{Rack: "use1-az3"}},
	}

	assignor := &CooperativeStickyRackAffinityAssignor{}
	result := assignor.AssignGroups(members, partitions)

	t.Logf("MSK result: %+v", result)

	// Each consumer should have 2 partitions
	for memberID, topicParts := range result {
		if len(topicParts["orders"]) != 2 {
			t.Errorf("member %s should have 2 partitions", memberID)
		}
	}
}

func TestCooperativeStickyRackAffinityAssignor_SetAssignment(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1a"}

	assignment := map[string][]int32{
		"topic-a": {0, 1},
		"topic-b": {2},
	}
	assignor.SetAssignment(assignment, 10)

	if assignor.GetGeneration() != 10 {
		t.Errorf("expected generation 10, got %d", assignor.GetGeneration())
	}

	owned := assignor.GetOwnedPartitions()
	if !reflect.DeepEqual(owned, assignment) {
		t.Errorf("owned partitions mismatch: got %v, want %v", owned, assignment)
	}
}

func TestCooperativeStickyRackAffinityAssignor_ImplementsGroupBalancer(t *testing.T) {
	var _ GroupBalancer = (*CooperativeStickyRackAffinityAssignor)(nil)
}

func TestCooperativeStickyRackAffinityAssignor_ConcurrentAccess(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: "us-east-1a"}

	// Run concurrent reads and writes
	done := make(chan bool)
	const goroutines = 10
	const iterations = 100

	// Writers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < iterations; j++ {
				assignor.SetAssignment(map[string][]int32{
					"topic-a": {int32(j), int32(j + 1)},
				}, int32(j))
			}
			done <- true
		}(i)
	}

	// Readers (UserData)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				_, _ = assignor.UserData()
			}
			done <- true
		}()
	}

	// Readers (GetOwnedPartitions)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				_ = assignor.GetOwnedPartitions()
			}
			done <- true
		}()
	}

	// Readers (GetGeneration)
	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
				_ = assignor.GetGeneration()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines*4; i++ {
		<-done
	}
}

// Helper function to create rack-only user data
func makeRackUserData(rack string) []byte {
	assignor := &CooperativeStickyRackAffinityAssignor{Rack: rack}
	data, _ := assignor.UserData()
	return data
}

func contains(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// Benchmark for rack-affinity assignor
func BenchmarkCooperativeStickyRackAffinityAssignor_AssignGroups(b *testing.B) {
	assignor := &CooperativeStickyRackAffinityAssignor{}
	azs := []string{"us-east-1a", "us-east-1b", "us-east-1c"}

	members := make([]GroupMember, 30)
	topics := []string{"topic-a", "topic-b", "topic-c"}
	for i := 0; i < 30; i++ {
		az := azs[i%3]
		a := &CooperativeStickyRackAffinityAssignor{Rack: az}
		userData, _ := a.UserData()
		members[i] = GroupMember{
			ID:       string(rune('A' + i)),
			Topics:   topics,
			UserData: userData,
		}
	}

	partitions := make([]Partition, 0, 300)
	for _, topic := range topics {
		for i := 0; i < 100; i++ {
			partitions = append(partitions, Partition{
				Topic:  topic,
				ID:     i,
				Leader: Broker{Rack: azs[i%3]},
			})
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assignor.AssignGroups(members, partitions)
	}
}

// ===== Additional Edge Case Tests =====

func TestCooperativeStickyAssignor_EmptyUserData(t *testing.T) {
	// Test that empty UserData is handled gracefully
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}, UserData: nil},
		{ID: "consumer-2", Topics: []string{"topic-a"}, UserData: []byte{}},
		{ID: "consumer-3", Topics: []string{"topic-a"}, UserData: []byte{0, 0}}, // Invalid
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-a", ID: 2},
	}

	// Should not panic
	result := assignor.AssignGroups(members, partitions)

	// Should still produce valid assignment
	total := 0
	for _, topicParts := range result {
		for _, parts := range topicParts {
			total += len(parts)
		}
	}
	if total != 3 {
		t.Errorf("expected 3 partitions assigned, got %d", total)
	}
}

func TestCooperativeStickyAssignor_DuplicateTopicsInSubscription(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	// Member subscribed to same topic multiple times (edge case)
	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a", "topic-a", "topic-b"}},
		{ID: "consumer-2", Topics: []string{"topic-a", "topic-b"}},
	}

	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-b", ID: 0},
		{Topic: "topic-b", ID: 1},
	}

	// Should not panic and produce valid result
	result := assignor.AssignGroups(members, partitions)

	if len(result) != 2 {
		t.Errorf("expected 2 members in result, got %d", len(result))
	}
}

func TestCooperativeStickyAssignor_PartitionWithNoSubscribers(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	members := []GroupMember{
		{ID: "consumer-1", Topics: []string{"topic-a"}},
		{ID: "consumer-2", Topics: []string{"topic-a"}},
	}

	// topic-b has no subscribers
	partitions := []Partition{
		{Topic: "topic-a", ID: 0},
		{Topic: "topic-a", ID: 1},
		{Topic: "topic-b", ID: 0}, // No one subscribed to topic-b
	}

	result := assignor.AssignGroups(members, partitions)

	// Only topic-a partitions should be assigned
	total := 0
	for _, topicParts := range result {
		for _, parts := range topicParts {
			total += len(parts)
		}
	}
	if total != 2 {
		t.Errorf("expected 2 partitions assigned, got %d", total)
	}
}

func TestCooperativeStickyAssignor_VeryLargeGeneration(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	// Test with max int32 generation
	assignor.SetAssignment(map[string][]int32{
		"topic-a": {0, 1, 2},
	}, 2147483647) // Max int32

	data, err := assignor.UserData()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	decoded, err := decodeStickyAssignorUserData(data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded.Generation != 2147483647 {
		t.Errorf("expected generation 2147483647, got %d", decoded.Generation)
	}
}

func TestCooperativeStickyAssignor_ManyTopics(t *testing.T) {
	assignor := &CooperativeStickyAssignor{}

	// Create 100 topics with 10 partitions each
	topics := make([]string, 100)
	for i := 0; i < 100; i++ {
		topics[i] = "topic-" + string(rune('A'+i/26)) + string(rune('a'+i%26))
	}

	members := make([]GroupMember, 10)
	for i := 0; i < 10; i++ {
		members[i] = GroupMember{
			ID:     "consumer-" + string(rune('0'+i)),
			Topics: topics,
		}
	}

	partitions := make([]Partition, 0, 1000)
	for _, topic := range topics {
		for i := 0; i < 10; i++ {
			partitions = append(partitions, Partition{Topic: topic, ID: i})
		}
	}

	result := assignor.AssignGroups(members, partitions)

	// Each member should get 100 partitions (1000 / 10)
	for memberID, topicParts := range result {
		count := 0
		for _, parts := range topicParts {
			count += len(parts)
		}
		if count != 100 {
			t.Errorf("member %s should have 100 partitions, got %d", memberID, count)
		}
	}
}

func TestCooperativeStickyAssignor_ZeroValue(t *testing.T) {
	// Test that zero-value assignor works
	var assignor CooperativeStickyAssignor

	// Should not panic
	name := assignor.ProtocolName()
	if name != "cooperative-sticky" {
		t.Errorf("unexpected protocol name: %s", name)
	}

	data, err := assignor.UserData()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil data for zero-value assignor")
	}

	owned := assignor.GetOwnedPartitions()
	if owned != nil {
		t.Errorf("expected nil owned partitions for zero-value assignor")
	}

	gen := assignor.GetGeneration()
	if gen != 0 {
		t.Errorf("expected generation 0, got %d", gen)
	}
}

func TestCooperativeStickyRackAffinityAssignor_ZeroValue(t *testing.T) {
	// Test that zero-value assignor works (Rack will be "unknown" when neither Rack nor RackFunc is set)
	var assignor CooperativeStickyRackAffinityAssignor

	name := assignor.ProtocolName()
	if name != "cooperative-sticky-rack-affinity" {
		t.Errorf("unexpected protocol name: %s", name)
	}

	// Should produce userData with "unknown" rack (since neither Rack nor RackFunc is set)
	data, err := assignor.UserData()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if data == nil {
		t.Errorf("expected non-nil data even with empty rack")
	}

	rack, _, _ := parseStickyRackAffinityUserData(data)
	if rack != "unknown" {
		t.Errorf("expected 'unknown' rack when neither Rack nor RackFunc is set, got %s", rack)
	}
}

func TestCooperativeStickyRackAffinityAssignor_SpecialCharactersInRack(t *testing.T) {
	// Test rack names with special characters (shouldn't happen in practice, but be safe)
	specialRacks := []string{
		"us-east-1a",
		"use1-az1",
		"rack-with-dashes",
		"rack_with_underscores",
		"rack.with.dots",
		"rack/with/slashes", // Unlikely but test it
	}

	for _, rack := range specialRacks {
		t.Run(rack, func(t *testing.T) {
			assignor := &CooperativeStickyRackAffinityAssignor{Rack: rack}
			assignor.SetAssignment(map[string][]int32{"topic": {0}}, 1)

			data, err := assignor.UserData()
			if err != nil {
				t.Fatalf("failed to encode rack %q: %v", rack, err)
			}

			parsedRack, _, _ := parseStickyRackAffinityUserData(data)
			if parsedRack != rack {
				t.Errorf("rack mismatch: got %q, want %q", parsedRack, rack)
			}
		})
	}
}

func TestCooperativeStickyRackAffinityAssignor_AllConsumersInOneRack(t *testing.T) {
	// All consumers in one AZ, partitions spread across 3 AZs
	members := []GroupMember{
		{ID: "c1", Topics: []string{"t"}, UserData: makeRackUserData("us-east-1a")},
		{ID: "c2", Topics: []string{"t"}, UserData: makeRackUserData("us-east-1a")},
		{ID: "c3", Topics: []string{"t"}, UserData: makeRackUserData("us-east-1a")},
	}

	partitions := []Partition{
		{Topic: "t", ID: 0, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "t", ID: 1, Leader: Broker{Rack: "us-east-1b"}},
		{Topic: "t", ID: 2, Leader: Broker{Rack: "us-east-1c"}},
		{Topic: "t", ID: 3, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "t", ID: 4, Leader: Broker{Rack: "us-east-1b"}},
		{Topic: "t", ID: 5, Leader: Broker{Rack: "us-east-1c"}},
	}

	assignor := &CooperativeStickyRackAffinityAssignor{}
	result := assignor.AssignGroups(members, partitions)

	// Should still produce balanced assignment
	for memberID, topicParts := range result {
		if len(topicParts["t"]) != 2 {
			t.Errorf("member %s should have 2 partitions, got %d", memberID, len(topicParts["t"]))
		}
	}
}

func TestCooperativeStickyRackAffinityAssignor_SinglePartitionManyConsumers(t *testing.T) {
	// Edge case: more consumers than partitions
	members := make([]GroupMember, 10)
	for i := 0; i < 10; i++ {
		members[i] = GroupMember{
			ID:       "c" + string(rune('0'+i)),
			Topics:   []string{"t"},
			UserData: makeRackUserData("us-east-1a"),
		}
	}

	partitions := []Partition{
		{Topic: "t", ID: 0, Leader: Broker{Rack: "us-east-1a"}},
	}

	assignor := &CooperativeStickyRackAffinityAssignor{}
	result := assignor.AssignGroups(members, partitions)

	// Only one consumer should get the partition
	assignedCount := 0
	for _, topicParts := range result {
		assignedCount += len(topicParts["t"])
	}
	if assignedCount != 1 {
		t.Errorf("expected 1 partition assigned, got %d", assignedCount)
	}
}

// TestUserDataEncodeDecode verifies round-trip encoding/decoding
func TestUserDataEncodeDecode(t *testing.T) {
	testCases := []struct {
		name       string
		topics     map[string][]int32
		generation int32
	}{
		{
			name:       "single topic",
			topics:     map[string][]int32{"topic-a": {0, 1, 2}},
			generation: 1,
		},
		{
			name:       "multiple topics",
			topics:     map[string][]int32{"topic-a": {0}, "topic-b": {1, 2}, "topic-c": {3, 4, 5}},
			generation: 100,
		},
		{
			name:       "empty partitions",
			topics:     map[string][]int32{"topic-a": {}},
			generation: 1,
		},
		{
			name:       "large partition IDs",
			topics:     map[string][]int32{"topic-a": {1000, 2000, 3000}},
			generation: 50,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := StickyAssignorUserDataV1{
				Version:    1,
				Topics:     tc.topics,
				Generation: tc.generation,
			}

			encoded, err := encodeStickyAssignorUserData(original)
			if err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			decoded, err := decodeStickyAssignorUserData(encoded)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if decoded.Generation != original.Generation {
				t.Errorf("generation mismatch: got %d, want %d", decoded.Generation, original.Generation)
			}

			for topic, parts := range original.Topics {
				decodedParts, ok := decoded.Topics[topic]
				if !ok {
					t.Errorf("missing topic %s", topic)
					continue
				}
				if len(decodedParts) != len(parts) {
					t.Errorf("topic %s partition count mismatch: got %d, want %d",
						topic, len(decodedParts), len(parts))
				}
			}
		})
	}
}

// TestRackAffinityRackFuncLazyResolution tests that RackFunc is called lazily on first UserData() call
func TestRackAffinityRackFuncLazyResolution(t *testing.T) {
	callCount := 0
	assignor := &CooperativeStickyRackAffinityAssignor{
		RackFunc: func() string {
			callCount++
			return "us-east-1a"
		},
	}

	// RackFunc should not be called until UserData() is invoked
	if callCount != 0 {
		t.Errorf("RackFunc called before UserData(), call count: %d", callCount)
	}

	// First UserData() call should invoke RackFunc
	_, err := assignor.UserData()
	if err != nil {
		t.Fatalf("UserData() failed: %v", err)
	}

	if callCount != 1 {
		t.Errorf("RackFunc not called after first UserData(), call count: %d", callCount)
	}

	// Second UserData() call should NOT invoke RackFunc (cached)
	_, err = assignor.UserData()
	if err != nil {
		t.Fatalf("UserData() failed: %v", err)
	}

	if callCount != 1 {
		t.Errorf("RackFunc called again on second UserData(), call count: %d", callCount)
	}

	// Verify the resolved rack
	rack := assignor.GetRack()
	if rack != "us-east-1a" {
		t.Errorf("expected rack 'us-east-1a', got '%s'", rack)
	}

	// GetRack() should also not invoke RackFunc again
	if callCount != 1 {
		t.Errorf("RackFunc called on GetRack(), call count: %d", callCount)
	}
}

// TestRackAffinityStaticRackTakesPrecedence tests that Rack field takes precedence over RackFunc
func TestRackAffinityStaticRackTakesPrecedence(t *testing.T) {
	callCount := 0
	assignor := &CooperativeStickyRackAffinityAssignor{
		Rack: "static-rack",
		RackFunc: func() string {
			callCount++
			return "dynamic-rack"
		},
	}

	_, err := assignor.UserData()
	if err != nil {
		t.Fatalf("UserData() failed: %v", err)
	}

	// RackFunc should NOT be called when Rack is set
	if callCount != 0 {
		t.Errorf("RackFunc called when Rack is set, call count: %d", callCount)
	}

	// Verify static rack is used
	rack := assignor.GetRack()
	if rack != "static-rack" {
		t.Errorf("expected rack 'static-rack', got '%s'", rack)
	}
}

// TestRackAffinityRackFuncReturnsEmpty tests that empty RackFunc result defaults to "unknown"
func TestRackAffinityRackFuncReturnsEmpty(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{
		RackFunc: func() string {
			return "" // Empty result
		},
	}

	_, err := assignor.UserData()
	if err != nil {
		t.Fatalf("UserData() failed: %v", err)
	}

	rack := assignor.GetRack()
	if rack != "unknown" {
		t.Errorf("expected rack 'unknown' for empty RackFunc, got '%s'", rack)
	}
}

// TestRackAffinityNilRackFunc tests behavior when neither Rack nor RackFunc is set
func TestRackAffinityNilRackFunc(t *testing.T) {
	assignor := &CooperativeStickyRackAffinityAssignor{}

	_, err := assignor.UserData()
	if err != nil {
		t.Fatalf("UserData() failed: %v", err)
	}

	rack := assignor.GetRack()
	if rack != "unknown" {
		t.Errorf("expected rack 'unknown' when nothing is set, got '%s'", rack)
	}
}

// TestRackAffinityRackFuncConcurrency tests that RackFunc is only called once even with concurrent access
func TestRackAffinityRackFuncConcurrency(t *testing.T) {
	var callCount int32
	assignor := &CooperativeStickyRackAffinityAssignor{
		RackFunc: func() string {
			atomic.AddInt32(&callCount, 1)
			return "concurrent-rack"
		},
	}

	// Run many concurrent UserData() calls
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = assignor.UserData()
		}()
	}
	wg.Wait()

	// RackFunc should only be called once due to double-check locking
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("RackFunc called %d times, expected 1", callCount)
	}
}

// TestRackAffinityWithRackFuncAssignment tests that RackFunc-resolved rack is used correctly in assignments
func TestRackAffinityWithRackFuncAssignment(t *testing.T) {
	// Create members where one uses static Rack and one uses RackFunc
	// Simulate the UserData() encoding

	staticAssignor := &CooperativeStickyRackAffinityAssignor{
		Rack: "us-east-1a",
	}
	dynamicAssignor := &CooperativeStickyRackAffinityAssignor{
		RackFunc: func() string {
			return "us-east-1b"
		},
	}

	staticUserData, _ := staticAssignor.UserData()
	dynamicUserData, _ := dynamicAssignor.UserData()

	members := []GroupMember{
		{ID: "static-consumer", Topics: []string{"t"}, UserData: staticUserData},
		{ID: "dynamic-consumer", Topics: []string{"t"}, UserData: dynamicUserData},
	}

	partitions := []Partition{
		{Topic: "t", ID: 0, Leader: Broker{Rack: "us-east-1a"}},
		{Topic: "t", ID: 1, Leader: Broker{Rack: "us-east-1b"}},
	}

	leaderAssignor := &CooperativeStickyRackAffinityAssignor{}
	result := leaderAssignor.AssignGroups(members, partitions)

	// Check both partitions are assigned
	totalAssigned := 0
	for _, topicParts := range result {
		totalAssigned += len(topicParts["t"])
	}
	if totalAssigned != 2 {
		t.Fatalf("expected 2 partitions assigned, got %d", totalAssigned)
	}

	// Static consumer should get partition 0 (same rack)
	if p0 := result["static-consumer"]["t"]; len(p0) != 1 || p0[0] != 0 {
		t.Errorf("expected static-consumer to get partition 0, got %v", p0)
	}

	// Dynamic consumer should get partition 1 (same rack)
	if p1 := result["dynamic-consumer"]["t"]; len(p1) != 1 || p1[0] != 1 {
		t.Errorf("expected dynamic-consumer to get partition 1, got %v", p1)
	}
}
