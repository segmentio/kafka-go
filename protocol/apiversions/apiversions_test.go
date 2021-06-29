package apiversions_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
	v2 = 2
)

func TestApiversionsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &apiversions.Request{})

	prototest.TestRequest(t, v1, &apiversions.Request{})

	prototest.TestRequest(t, v2, &apiversions.Request{})
}

func TestApiversionsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &apiversions.Response{
		ErrorCode: 0,
		ApiKeys: []apiversions.ApiKeyResponse{
			{
				ApiKey:     0,
				MinVersion: 0,
				MaxVersion: 2,
			},
		},
	})

	prototest.TestResponse(t, v1, &apiversions.Response{
		ErrorCode: 0,
		ApiKeys: []apiversions.ApiKeyResponse{
			{
				ApiKey:     0,
				MinVersion: 0,
				MaxVersion: 2,
			},
		},
		ThrottleTimeMs: 10,
	})

	prototest.TestResponse(t, v2, &apiversions.Response{
		ErrorCode: 0,
		ApiKeys: []apiversions.ApiKeyResponse{
			{
				ApiKey:     0,
				MinVersion: 0,
				MaxVersion: 2,
			},
		},
		ThrottleTimeMs: 50,
	})
}
