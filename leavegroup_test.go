package kafka

import (
	"testing"
)

func TestLeaveGroupResponseV0(t *testing.T) {
	testProtocolType(t,
		&leaveGroupResponseV0{
			ErrorCode: 2,
		},
		&leaveGroupResponseV0{},
	)
}
