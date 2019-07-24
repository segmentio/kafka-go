package kafka

import (
	"testing"
)

func TestSASLAuthenticateRequestV0(t *testing.T) {
	testProtocolType(t,
		&saslAuthenticateRequestV0{
			Data: []byte("\x00user\x00pass"),
		},
		&saslAuthenticateRequestV0{},
	)
}

func TestSASLAuthenticateResponseV0(t *testing.T) {
	testProtocolType(t,
		&saslAuthenticateResponseV0{
			ErrorCode:    2,
			ErrorMessage: "Message",
			Data:         []byte("bytes"),
		},
		&saslAuthenticateResponseV0{},
	)
}
