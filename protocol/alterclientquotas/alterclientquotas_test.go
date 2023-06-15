package alterclientquotas_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/alterclientquotas"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
)

func TestAlterClientQuotasRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &alterclientquotas.Request{
		ValidateOnly: true,
		Entries: []alterclientquotas.Entry{
			{
				Entities: []alterclientquotas.Entity{
					{
						EntityType: "client-id",
						EntityName: "my-client-id",
					},
				},
				Ops: []alterclientquotas.Ops{
					{
						Key:    "producer_byte_rate",
						Value:  1.0,
						Remove: false,
					},
				},
			},
		},
	})

	prototest.TestRequest(t, v1, &alterclientquotas.Request{
		ValidateOnly: true,
		Entries: []alterclientquotas.Entry{
			{
				Entities: []alterclientquotas.Entity{
					{
						EntityType: "client-id",
						EntityName: "my-client-id",
					},
				},
				Ops: []alterclientquotas.Ops{
					{
						Key:    "producer_byte_rate",
						Value:  1.0,
						Remove: false,
					},
				},
			},
		},
	})
}

func TestAlterClientQuotasResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &alterclientquotas.Response{
		ThrottleTimeMs: 500,
		Results: []alterclientquotas.ResponseQuotas{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				Entities: []alterclientquotas.Entity{
					{
						EntityType: "client-id",
						EntityName: "my-client-id",
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v1, &alterclientquotas.Response{
		ThrottleTimeMs: 500,
		Results: []alterclientquotas.ResponseQuotas{
			{
				ErrorCode:    1,
				ErrorMessage: "foo",
				Entities: []alterclientquotas.Entity{
					{
						EntityType: "client-id",
						EntityName: "my-client-id",
					},
				},
			},
		},
	})
}
