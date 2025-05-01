package sasl_test

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	ktesting "github.com/segmentio/kafka-go/testing"
)

const (
	saslTestConnect = "localhost:9093" // connect to sasl listener
	saslTestTopic   = "test-writer-0"  // this topic is guaranteed to exist.
)

func TestSASL(t *testing.T) {
	scramUsers := map[scram.Algorithm]string{scram.SHA256: "adminscram", scram.SHA512: "adminscram"}
	// kafka 4.0.0 test environment supports only different users for different scram algorithms.
	if ktesting.KafkaIsAtLeast("4.0.0") {
		scramUsers = map[scram.Algorithm]string{scram.SHA256: "adminscram256", scram.SHA512: "adminscram512"}
	}
	tests := []struct {
		valid    func() sasl.Mechanism
		invalid  func() sasl.Mechanism
		minKafka string
	}{
		{
			valid: func() sasl.Mechanism {
				return plain.Mechanism{
					Username: "adminplain",
					Password: "admin-secret",
				}
			},
			invalid: func() sasl.Mechanism {
				return plain.Mechanism{
					Username: "adminplain",
					Password: "badpassword",
				}
			},
		},
		{
			valid: func() sasl.Mechanism {
				mech, _ := scram.Mechanism(scram.SHA256, scramUsers[scram.SHA256], "admin-secret-256")
				return mech
			},
			invalid: func() sasl.Mechanism {
				mech, _ := scram.Mechanism(scram.SHA256, scramUsers[scram.SHA256], "badpassword")
				return mech
			},
			minKafka: "0.10.2.0",
		},
		{
			valid: func() sasl.Mechanism {
				mech, _ := scram.Mechanism(scram.SHA512, scramUsers[scram.SHA512], "admin-secret-512")
				return mech
			},
			invalid: func() sasl.Mechanism {
				mech, _ := scram.Mechanism(scram.SHA512, scramUsers[scram.SHA512], "badpassword")
				return mech
			},
			minKafka: "0.10.2.0",
		},
	}

	for _, tt := range tests {
		mech := tt.valid()
		if !ktesting.KafkaIsAtLeast(tt.minKafka) {
			t.Skip("requires min kafka version " + tt.minKafka)
		}

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
	_, err := d.DialLeader(ctx, "tcp", saslTestConnect, saslTestTopic, 0)
	if success && err != nil {
		t.Errorf("should have logged in correctly, got err: %v", err)
	} else if !success && err == nil {
		t.Errorf("should not have logged in correctly")
	}
}
