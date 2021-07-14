package deletetopics_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/deletetopics"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
	v3 = 3
)

func TestDeleteTopicsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &deletetopics.Request{
		TopicNames: []string{"foo", "bar"},
		TimeoutMs:  500,
	})

	prototest.TestRequest(t, v1, &deletetopics.Request{
		TopicNames: []string{"foo", "bar"},
		TimeoutMs:  500,
	})

	prototest.TestRequest(t, v3, &deletetopics.Request{
		TopicNames: []string{"foo", "bar"},
		TimeoutMs:  500,
	})
}

func TestDeleteTopicsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &deletetopics.Response{
		Responses: []deletetopics.ResponseTopic{
			{
				Name:      "foo",
				ErrorCode: 1,
			},
			{
				Name:      "bar",
				ErrorCode: 1,
			},
		},
	})

	prototest.TestResponse(t, v1, &deletetopics.Response{
		ThrottleTimeMs: 500,
		Responses: []deletetopics.ResponseTopic{
			{
				Name:      "foo",
				ErrorCode: 1,
			},
			{
				Name:      "bar",
				ErrorCode: 1,
			},
		},
	})

	prototest.TestResponse(t, v3, &deletetopics.Response{
		ThrottleTimeMs: 500,
		Responses: []deletetopics.ResponseTopic{
			{
				Name:      "foo",
				ErrorCode: 1,
			},
			{
				Name:      "bar",
				ErrorCode: 1,
			},
		},
	})
}
