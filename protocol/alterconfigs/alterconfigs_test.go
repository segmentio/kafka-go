package alterconfigs_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/alterconfigs"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
)

func TestAlterConfigsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &alterconfigs.Request{
		ValidateOnly: true,
		Resources: []alterconfigs.RequestResources{
			{
				ResourceType: 1,
				ResourceName: "foo",
				Configs: []alterconfigs.RequestConfig{
					{
						Name:  "foo",
						Value: "foo",
					},
				},
			},
		},
	})

	prototest.TestRequest(t, v1, &alterconfigs.Request{
		ValidateOnly: true,
		Resources: []alterconfigs.RequestResources{
			{
				ResourceType: 1,
				ResourceName: "foo",
				Configs: []alterconfigs.RequestConfig{
					{
						Name:  "foo",
						Value: "foo",
					},
				},
			},
		},
	})
}

func TestAlterConfigsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &alterconfigs.Response{
		ThrottleTimeMs: 500,
		Responses: []alterconfigs.ResponseResponses{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				ResourceType: 1,
				ResourceName: "foo",
			},
		},
	})

	prototest.TestResponse(t, v1, &alterconfigs.Response{
		ThrottleTimeMs: 500,
		Responses: []alterconfigs.ResponseResponses{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				ResourceType: 1,
				ResourceName: "foo",
			},
		},
	})
}
