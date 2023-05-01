package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestClientAlterClientQuotas(t *testing.T) {
	// Added in Version 2.6.0 https://issues.apache.org/jira/browse/KAFKA-7740
	if !ktesting.KafkaIsAtLeast("2.6.0") {
		return
	}

	const (
		entityType = "client-id"
		entityName = "my-client-id"
		key        = "producer_byte_rate"
		value      = 500000.0
	)

	client, shutdown := newLocalClient()
	defer shutdown()

	expectedAlterResp := AlterClientQuotasResponse{
		Throttle: 0,
		Entries: []AlterClientQuotaResponseQuotas{
			{
				ErrorCode: 0,
				Entities: []AlterClientQuotaEntity{
					{
						EntityName: entityName,
						EntityType: entityType,
					},
				},
			},
		},
	}

	alterResp, err := client.AlterClientQuotas(context.Background(), &AlterClientQuotasRequest{
		Entries: []AlterClientQuotaEntry{
			{
				Entities: []AlterClientQuotaEntity{
					{
						EntityType: entityType,
						EntityName: entityName,
					},
				},
				Ops: []AlterClientQuotaOps{
					{
						Key:    key,
						Value:  value,
						Remove: false,
					},
				},
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expectedAlterResp, *alterResp)

	describeResp, err := client.DescribeClientQuotas(context.Background(), &DescribeClientQuotasRequest{
		Components: []DescribeClientQuotasRequestComponent{
			{
				EntityType: entityType,
				MatchType:  0,
				Match:      entityName,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	expectedDescribeResp := DescribeClientQuotasResponse{
		Throttle:  0,
		ErrorCode: 0,
		Entries: []DescribeClientQuotasResponseQuotas{
			{
				Entities: []DescribeClientQuotasEntity{
					{
						EntityType: entityType,
						EntityName: entityName,
					},
				},
				Values: []DescribeClientQuotasValue{
					{
						Key:   key,
						Value: value,
					},
				},
			},
		},
	}

	assert.Equal(t, expectedDescribeResp, *describeResp)
}
