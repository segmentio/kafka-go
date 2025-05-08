package azure_event_hubs_entra

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/segmentio/kafka-go/sasl"
)

type MockTokenCredential struct {
	getTokenFunc func() (string, error)
}

func (c *MockTokenCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if len(options.Scopes) != 1 {
		return azcore.AccessToken{}, fmt.Errorf("Scopes must contain 1 element! Contains %d elements.", len(options.Scopes))
	}

	scope := options.Scopes[0]

	if !strings.HasPrefix(scope, "https://") {
		return azcore.AccessToken{}, fmt.Errorf("Scope must start with https, and it did not.")
	}

	if !strings.HasSuffix(scope, "/.default") {
		return azcore.AccessToken{}, fmt.Errorf("Scope must end with /.default, and it did not.")
	}

	if options.EnableCAE {
		return azcore.AccessToken{}, fmt.Errorf("CAE must be false. It was true.")
	}

	token, err := c.getTokenFunc()

	if err != nil {
		return azcore.AccessToken{}, err
	}

	return azcore.AccessToken{Token: token}, nil
}

func TestName(t *testing.T) {
	mechanism := NewMechanism(&MockTokenCredential{
		getTokenFunc: func() (string, error) { return "testtoken", nil },
	})

	expected := "OAUTHBEARER"
	actual := mechanism.Name()

	if expected != actual {
		t.Fatalf("Expected: %s - Actual: %s", expected, actual)
	}
}

func TestStart(t *testing.T) {
	mechanism := NewMechanism(&MockTokenCredential{
		getTokenFunc: func() (string, error) { return "testtoken", nil },
	})

	ctx := sasl.WithMetadata(context.Background(), &sasl.Metadata{
		Host: "test.servicebus.windows.net",
		Port: 9093,
	})

	stateMachine, saslBytes, err := mechanism.Start(ctx)

	if stateMachine == nil {
		t.Fatalf("Expected stateMachine to be non-nil")
	}

	expectedSaslData := "n,,\x01auth=Bearer testtoken\x01\x01"

	if string(saslBytes) != expectedSaslData {
		t.Fatalf("expected saslData to be %s. Received %s.", expectedSaslData, string(saslBytes))
	}

	if err != nil {
		t.Fatalf("expected err to be nil")
	}
}

func TestStartNoMetadata(t *testing.T) {
	mechanism := NewMechanism(&MockTokenCredential{
		getTokenFunc: func() (string, error) { return "testtoken", nil },
	})

	ctx := context.Background()

	stateMachine, saslBytes, err := mechanism.Start(ctx)

	assertStartError(stateMachine, t, saslBytes, err, "missing sasl metadata")
}

func TestStartTokenError(t *testing.T) {
	mechanism := NewMechanism(&MockTokenCredential{
		getTokenFunc: func() (string, error) { return "", errors.New("Failed to acquire token") },
	})

	ctx := sasl.WithMetadata(context.Background(), &sasl.Metadata{
		Host: "test.servicebus.windows.net",
		Port: 9093,
	})

	stateMachine, saslBytes, err := mechanism.Start(ctx)

	assertStartError(stateMachine, t, saslBytes, err, "failed to request an Azure Entra Token: Failed to acquire token")
}

func assertStartError(stateMachine sasl.StateMachine, t *testing.T, saslBytes []byte, err error, expectedError string) {
	if stateMachine != nil {
		t.Fatalf("Expected stateMachine to be nil")
	}

	if saslBytes != nil {
		t.Fatalf("Expected saslBytes to be nil")
	}

	if err.Error() != expectedError {
		t.Fatalf("expected err to be %s. was %s", expectedError, err.Error())
	}
}

func TestNext(t *testing.T) {
	mechanism := NewMechanism(&MockTokenCredential{
		getTokenFunc: func() (string, error) { return "testtoken", nil },
	})

	done, response, err := mechanism.Next(context.Background(), []byte("challenge"))

	if !done {
		t.Fatalf("Expected done to be true")
	}

	if response != nil {
		t.Fatalf("Expected nil response")
	}

	if err != nil {
		t.Fatalf("Expected nil error")
	}
}
