package kafka

// RebalanceEventInterceptor defines the rebalance event callback API
type RebalanceEventInterceptor interface {
	Callback(map[string][]PartitionAssignment)
}

type RebalanceFunc func(map[string][]PartitionAssignment)

func (f RebalanceFunc) Callback(partitionAssignments map[string][]PartitionAssignment) {
	f(partitionAssignments)
}
