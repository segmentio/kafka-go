package oauthbearer_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/oauthbearer"
)

const (
	oauthbearerTestConnect = "localhost:9094"
	oauthbearerTestTopic   = "test-writer-0"
)

// unsecuredToken generates a test token compatible with Kafka's
// OAuthBearerUnsecuredValidatorCallbackHandler.
func unsecuredToken(subject string) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
		now := time.Now().Unix()
		payload := fmt.Sprintf(`{"sub":"%s","iat":%d,"exp":%d}`, subject, now, now+3600)
		payloadB64 := base64.RawURLEncoding.EncodeToString([]byte(payload))
		return header + "." + payloadB64 + ".", nil
	}
}

// expiredToken generates an already-expired test token (invalid credentials equivalent).
func expiredToken(subject string) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
		past := time.Now().Add(-1 * time.Hour).Unix()
		payload := fmt.Sprintf(`{"sub":"%s","iat":%d,"exp":%d}`, subject, past, past+1)
		payloadB64 := base64.RawURLEncoding.EncodeToString([]byte(payload))
		return header + "." + payloadB64 + ".", nil
	}
}

func TestOAUTHBEARER(t *testing.T) {
	// Integration tests require Kafka with OAUTHBEARER listener on port 9094.
	// See docker-compose.yml for the required configuration.
	if os.Getenv("KAFKA_OAUTHBEARER_TEST") == "" {
		t.Skip("KAFKA_OAUTHBEARER_TEST not set; skipping integration test")
	}

	tests := []struct {
		valid   func() sasl.Mechanism
		invalid func() sasl.Mechanism
	}{
		{
			valid: func() sasl.Mechanism {
				return &oauthbearer.Mechanism{
					TokenFunc: unsecuredToken("testuser"),
				}
			},
			invalid: func() sasl.Mechanism {
				return &oauthbearer.Mechanism{
					TokenFunc: expiredToken("testuser"),
				}
			},
		},
	}

	for _, tt := range tests {
		mech := tt.valid()

		t.Run(mech.Name()+" success", func(t *testing.T) {
			testConnect(t, tt.valid(), true)
		})
		t.Run(mech.Name()+" failure", func(t *testing.T) {
			testConnect(t, tt.invalid(), false)
		})
		t.Run(mech.Name()+" is reusable", func(t *testing.T) {
			mech := tt.valid()
			testConnect(t, mech, true)
			testConnect(t, mech, true)
			testConnect(t, mech, true)
		})
	}
}

func testConnect(t *testing.T, mechanism sasl.Mechanism, success bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d := kafka.Dialer{
		SASLMechanism: mechanism,
	}
	conn, err := d.DialLeader(ctx, "tcp", oauthbearerTestConnect, oauthbearerTestTopic, 0)
	if success && err != nil {
		t.Errorf("should have logged in correctly, got err: %v", err)
	} else if !success && err == nil {
		conn.Close()
		t.Errorf("should not have logged in correctly")
	} else if err == nil {
		conn.Close()
	}
}
