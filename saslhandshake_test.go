package kafka

import (
	"testing"
)

func TestSASLHandshakeRequestV0(t *testing.T) {
	testProtocolType(t,
		&saslHandshakeRequestV0{
			Mechanism: "SCRAM-SHA-512",
		},
		&saslHandshakeRequestV0{},
	)
}

func TestSASLHandshakeResponseV0(t *testing.T) {
	testProtocolType(t,
		&saslHandshakeResponseV0{
			ErrorCode:         2,
			EnabledMechanisms: []string{"PLAIN", "SCRAM-SHA-512"},
		},
		&saslHandshakeResponseV0{},
	)
}
