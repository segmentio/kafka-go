package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
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

	resp, err := client.AlterClientQuotas(context.Background(), &AlterClientQuotasRequest{
		Entries: []AlterClientQuotaEntry{
			AlterClientQuotaEntry{
				Entities: []AlterClientQuotaEntity{
					AlterClientQuotaEntity{
						EntityType: entityType,
						EntityName: entityName,
					},
				},
				Ops: []AlterClientQuotaOps{
					AlterClientQuotaOps{
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
}
