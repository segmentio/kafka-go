package describeclientquotas_test

import (
	"testing"

	"github.com/segmentio/kafka-go/protocol/describeclientquotas"
	"github.com/segmentio/kafka-go/protocol/prototest"
)

const (
	v0 = 0
	v1 = 1
)

func TestDescribeClientQuotasRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &describeclientquotas.Request{
		Strict: true,
		Components: []describeclientquotas.Component{
			{
				EntityType: "client-id",
				MatchType:  0,
				Match:      "my-client-id",
			},
		},
	})

	prototest.TestRequest(t, v1, &describeclientquotas.Request{
		Strict: true,
		Components: []describeclientquotas.Component{
			{
				EntityType: "client-id",
				MatchType:  0,
				Match:      "my-client-id",
			},
		},
	})
}

func TestDescribeClientQuotasResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &describeclientquotas.Response{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   "foo",
		Entries: []describeclientquotas.ResponseQuotas{
			{
				Entities: []describeclientquotas.Entity{
					{
						EntityType: "client-id",
						EntityName: "my-client-id",
					},
				},
				Values: []describeclientquotas.Value{
					{
						Key:   "foo",
						Value: 1.0,
					},
				},
			},
		},
	})

	prototest.TestResponse(t, v1, &describeclientquotas.Response{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   "foo",
		Entries: []describeclientquotas.ResponseQuotas{
			{
				Entities: []describeclientquotas.Entity{
					{
						EntityType: "client-id",
						EntityName: "my-client-id",
					},
				},
				Values: []describeclientquotas.Value{
					{
						Key:   "foo",
						Value: 1.0,
					},
				},
			},
		},
	})
}
