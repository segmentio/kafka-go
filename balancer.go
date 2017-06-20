package kafka

import "sync/atomic"

type Balancer interface {
	Balance(key []byte, partitions ...int) (partition int)
}

type BalancerFunc func([]byte, ...int) int

func (f BalancerFunc) Balance(key []byte, partitions ...int) int {
	return f(key, partitions...)
}

type RoundRobin struct {
	offset uint32
}

func (rr *RoundRobin) Balance(key []byte, partitions ...int) int {
	return partitions[int(atomic.AddUint32(&rr.offset, 1))%len(partitions)]
}
