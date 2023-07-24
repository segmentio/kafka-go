package kafka

import (
	"context"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestAlterUserScramCredentials(t *testing.T) {
	// https://issues.apache.org/jira/browse/KAFKA-10259
	if !ktesting.KafkaIsAtLeast("2.7.0") {
		return
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	name := makeTopic()

	createRes, err := client.AlterUserScramCredentials(context.Background(), &AlterUserScramCredentialsRequest{
		Upsertions: []UserScramCredentialsUpsertion{
			{
				Name:           name,
				Mechanism:      ScramMechanismSha512,
				Iterations:     15000,
				Salt:           []byte("my-salt"),
				SaltedPassword: []byte("my-salted-password"),
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

	if len(createRes.Results) != 1 {
		t.Fatalf("expected 1 createResult; got %d", len(createRes.Results))
	}

	if createRes.Results[0].User != name {
		t.Fatalf("expected createResult with user: %s, got %s", name, createRes.Results[0].User)
	}

	deleteRes, err := client.AlterUserScramCredentials(context.Background(), &AlterUserScramCredentialsRequest{
		Deletions: []UserScramCredentialsDeletion{
			{
				Name:      name,
				Mechanism: ScramMechanismSha512,
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	for _, err := range deleteRes.Errors {
		if err != nil {
			t.Error(err)
		}
	}

	if len(deleteRes.Results) != 1 {
		t.Fatalf("expected 1 deleteResult; got %d", len(deleteRes.Results))
	}

	if deleteRes.Results[0].User != name {
		t.Fatalf("expected deleteResult with user: %s, got %s", name, deleteRes.Results[0].User)
	}
}
