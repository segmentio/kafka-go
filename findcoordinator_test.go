package kafka

import (
	"testing"
)

func TestFindCoordinatorResponseV0(t *testing.T) {
	testProtocolType(t,
		&findCoordinatorResponseV0{
			ErrorCode: 2,
			Coordinator: findCoordinatorResponseCoordinatorV0{
				NodeID: 3,
				Host:   "b",
				Port:   4,
			},
		},
		&findCoordinatorResponseV0{},
	)
}
