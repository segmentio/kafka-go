package heartbeat_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/heartbeat"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestHeartbeatRequest(t *testing.T) {
	versions := []int16{
		0, 1, 2, 3, 4,
	}

	for _, version := range versions {
		prototest.TestRequest(t, version, &heartbeat.Request{
			GroupID:      "test",
			GenerationID: 1,
			MemberID:     "member-1",
		})
	}
}

func TestHeartbeatResponse(t *testing.T) {
	versions := []int16{
		0, 1, 2, 3, 4,
	}

	for _, version := range versions {
		prototest.TestResponse(t, version, &heartbeat.Response{
			ErrorCode:      4,
			ThrottleTimeMs: 10,
		})
	}
}
