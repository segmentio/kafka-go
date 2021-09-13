package heartbeat_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/heartbeat"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

func TestHeartbeatRequest(t *testing.T) {
	// Versions 0-3 have all the same fields.
	for _, version := range []int16{0, 1, 2, 3} {
		prototest.TestRequest(t, version, &heartbeat.Request{
			GroupID:      "group-1",
			GenerationID: 1,
			MemberID:     "member-1",
		})
	}

	for _, version := range []int16{4} {
		prototest.TestRequest(t, version, &heartbeat.Request{
			GroupID:         "group-2",
			GenerationID:    10,
			MemberID:        "member-2",
			GroupInstanceID: "instace-1",
		})
	}
}

func TestHeartbeatResponse(t *testing.T) {
	for _, version := range []int16{0} {
		prototest.TestResponse(t, version, &heartbeat.Response{
			ErrorCode: 4,
		})
	}

	// Versions 1-4 have all the same fields.
	for _, version := range []int16{1, 2, 3, 4} {
		prototest.TestResponse(t, version, &heartbeat.Response{
			ErrorCode:      4,
			ThrottleTimeMs: 10,
		})
	}
}
