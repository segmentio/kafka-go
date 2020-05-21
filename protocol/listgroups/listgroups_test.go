package listgroups

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/prototest"
)

const v0 int16 = 0

func TestRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &Request{})
}

func TestResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &Response{
		ErrorCode: 1,
		Groups: []Group{
			{
				GroupID:      "g1",
				ProtocolType: "foo",
			},
			{
				GroupID:      "g2",
				ProtocolType: "bar",
			},
		},
	})
}
