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
}

func TestACLPermissionTypeMarshal(t *testing.T) {
	for i := ACLPermissionTypeUnknown; i <= ACLPermissionTypeAllow; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got ACLPermissionType
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to ACLPermissionType: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}

func TestACLOperationTypeMarshal(t *testing.T) {
	for i := ACLOperationTypeUnknown; i <= ACLOperationTypeIdempotentWrite; i++ {
		text, err := i.MarshalText()
		if err != nil {
			t.Errorf("couldn't marshal %d to text: %s", i, err)
		}
		var got ACLOperationType
		err = got.UnmarshalText(text)
		if err != nil {
			t.Errorf("couldn't unmarshal %s to ACLOperationType: %s", text, err)
		}
		if got != i {
			t.Errorf("got %d, want %d", got, i)
		}
	}
}
