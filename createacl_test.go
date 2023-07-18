package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestClientCreateACLs(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.0.1") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	var ACLs = []ACLEntry{
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
	}

	createRes, err := client.CreateACLs(context.Background(), &CreateACLsRequest{
		ACLs: ACLs,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, err := range createRes.Errors {
		if err != nil {
			t.Error(err)
		}
	}

	describeResp, err := client.DescribeACLs(context.Background(), &DescribeACLsRequest{
		Filters: []ACLFilter{
			{
				ResourceTypeFilter: ResourceTypeTopic,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	expectedDescribeResp := DescribeACLsResponse{
		Throttle: 0,
		Error:    makeError(0, ""),
		Resources: []ACLResource{
			{
				ResourceType: ResourceTypeTopic,
				ResourceName: "fake-topic-for-alice",
				ACLs: []ACLDescription{
					{
						Principal:      "User:alice",
						Host:           "*",
						Operation:      ACLOperationTypeRead,
						PermissionType: ACLPermissionTypeAllow,
					},
				},
			},
		},
	}

	assert.Equal(t, expectedDescribeResp, *describeResp)
}
