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

	if len(createRes.Results) != 1 {
		t.Fatalf("expected 1 createResult; got %d", len(createRes.Results))
	}

	if createRes.Results[0].User != name {
		t.Fatalf("expected createResult with user: %s, got %s", name, createRes.Results[0].User)
	}

	if createRes.Results[0].Error != nil {
		t.Fatalf("didn't expect an error in createResult, got %v", createRes.Results[0].Error)
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

	if len(deleteRes.Results) != 1 {
		t.Fatalf("expected 1 deleteResult; got %d", len(deleteRes.Results))
	}

	if deleteRes.Results[0].User != name {
		t.Fatalf("expected deleteResult with user: %s, got %s", name, deleteRes.Results[0].User)
	}

	if deleteRes.Results[0].Error != nil {
		t.Fatalf("didn't expect an error in deleteResult, got %v", deleteRes.Results[0].Error)
	}
}
