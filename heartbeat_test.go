package kafka

import (
	"testing"
)

func TestHeartbeatRequestV0(t *testing.T) {
	testProtocolType(t,
		&heartbeatResponseV0{
			ErrorCode: 2,
		},
		&heartbeatResponseV0{},
	)
}
