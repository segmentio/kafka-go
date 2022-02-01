package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientCreateACLs(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.0.1") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	res, err := client.CreateACLs(context.Background(), &CreateACLsRequest{
		ACLs: []ACLEntry{
			{
				Principal:           "User:alice",
				PermissionType:      ACLPermissionTypeAllow,
				Operation:           ACLOperationTypeRead,
				ResourceType:        ResourceTypeTopic,
				ResourcePatternType: PatternTypeLiteral,
				ResourceName:        "fake-topic-for-alice",
				Host:                "*",
			},
			{
				Principal:           "User:bob",
				PermissionType:      ACLPermissionTypeAllow,
				Operation:           ACLOperationTypeRead,
				ResourceType:        ResourceTypeGroup,
				ResourcePatternType: PatternTypeLiteral,
				ResourceName:        "fake-group-for-bob",
				Host:                "*",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, err := range res.Errors {
		if err != nil {
			t.Error(err)
		}
	}
}
