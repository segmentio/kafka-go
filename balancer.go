package kafka

// The Balancer interface provides an abstraction of the message distribution
// logic used by Writer instances to route messages to the partitions available
// on a kafka cluster.
//
// Instances of Balancer do not have to be safe to use concurrently by multiple
// goroutines, the Writer implementation ensures that calls to Balance are
// synchronized.
type Balancer interface {
	// Balance receives a message and a set of available partitions and
	// returns the partition number that the message should be routed to.
	Balance(msg Message, partitions ...int) (partition int)
}

// BalancerFunc is an implementation of the Balancer interface that makes it
// possible to use regular functions to distribute messages across paritions.
type BalancerFunc func(Message, ...int) int

// Balance calls f, satisfies the Balancer interface.
func (f BalancerFunc) Balance(msg Message, partitions ...int) int {
	return f(msg, partitions...)
}

// RoundRobin is an Balancer implementation that equally distributes messages
// across all available partitions.
type RoundRobin struct {
	offset uint64
}

// Balance satisfies the Balancer interface.
func (rr *RoundRobin) Balance(msg Message, partitions ...int) int {
	length := uint64(len(partitions))
	offset := rr.offset
	rr.offset++
	return partitions[offset%length]
}
