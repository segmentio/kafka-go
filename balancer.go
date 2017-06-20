package kafka

import "sort"

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
	//
	// An application should refrain from using a balancer to manage multiple
	// sets of partitions (from different topics for examples), use one balancer
	// instance for each partition set, so the balancer can detect when the
	// partitions change and assume that the kafka topic has been rebalanced.
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

// LeastBytes is a Balancer implementation that routes messages to the partition
// that has received the least amount of data.
//
// Note that no coordination is done between multiple producers, having good
// balancing relies on the fact that each producer using a LeastBytes balancer
// should produce well balanced messages.
type LeastBytes struct {
	counters []leastBytesCounter
}

type leastBytesCounter struct {
	partition int
	bytes     uint64
}

// Balance satisfies the Balancer interface.
func (lb *LeastBytes) Balance(msg Message, partitions ...int) int {
	for _, p := range partitions {
		if c := lb.counterOf(p); c == nil {
			lb.counters = lb.makeCounters(partitions...)
			break
		}
	}

	minBytes := lb.counters[0].bytes
	minIndex := 0

	for i, c := range lb.counters[1:] {
		if c.bytes < minBytes {
			minIndex = i + 1
			minBytes = c.bytes
		}
	}

	c := &lb.counters[minIndex]
	c.bytes += uint64(len(msg.Key)) + uint64(len(msg.Value))
	return c.partition
}

func (lb *LeastBytes) counterOf(partition int) *leastBytesCounter {
	i := sort.Search(len(lb.counters), func(i int) bool {
		return lb.counters[i].partition >= partition
	})
	if i == len(lb.counters) || lb.counters[i].partition != partition {
		return nil
	}
	return &lb.counters[i]
}

func (lb *LeastBytes) makeCounters(partitions ...int) (counters []leastBytesCounter) {
	counters = make([]leastBytesCounter, len(partitions))

	for i, p := range partitions {
		counters[i].partition = p
	}

	sort.Slice(counters, func(i int, j int) bool {
		return counters[i].partition < counters[j].partition
	})
	return
}
