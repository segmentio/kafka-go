package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestClientDescribeACLs(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("2.0.1") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	group := makeGroupID()

	createRes, err := client.CreateACLs(context.Background(), &CreateACLsRequest{
		ACLs: []ACLEntry{
			{
				Principal:           "User:alice",
				PermissionType:      ACLPermissionTypeAllow,
				Operation:           ACLOperationTypeRead,
				ResourceType:        ResourceTypeTopic,
				ResourcePatternType: PatternTypeLiteral,
				ResourceName:        topic,
				Host:                "*",
			},
			{
				Principal:           "User:bob",
				PermissionType:      ACLPermissionTypeAllow,
				Operation:           ACLOperationTypeRead,
				ResourceType:        ResourceTypeGroup,
				ResourcePatternType: PatternTypeLiteral,
				ResourceName:        group,
				Host:                "*",
			},
		},
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
		Filter: ACLFilter{
			ResourceTypeFilter:        ResourceTypeTopic,
			ResourceNameFilter:        topic,
			ResourcePatternTypeFilter: PatternTypeLiteral,
			Operation:                 ACLOperationTypeRead,
			PermissionType:            ACLPermissionTypeAllow,
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
				ResourceName: topic,
				PatternType:  PatternTypeLiteral,
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
