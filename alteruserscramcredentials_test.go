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

	res, err := client.AlterUserScramCredentials(context.Background(), &AlterUserScramCredentialsRequest{
		Upsertions: []UserScramCredentialsUpsertion{
			UserScramCredentialsUpsertion{
				Name:           "alice",
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

	for _, err := range res.Errors {
		if err != nil {
			t.Error(err)
		}
	}

	if len(res.Results) != 1 {
		t.Fatalf("expected 1 result; got %d", len(res.Results))
	}

	if res.Results[0].User != "alice" {
		t.Fatalf("expected result with user alice, got %s", res.Results[0].User)
	}
}
