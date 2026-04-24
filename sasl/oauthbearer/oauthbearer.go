package oauthbearer

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go/sasl"
)

// Mechanism implements the OAUTHBEARER mechanism and passes the token.
type Mechanism struct {
	Token string
}

func (Mechanism) Name() string {
	return "OAUTHBEARER"
}

func (m Mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	if m.Token == "" {
		return nil, nil, errors.New("token must have a value")
	}
	header := fmt.Sprintf("n,,\x01auth=Bearer %s\x01\x01", m.Token)
	byteArrayHeader := []byte(header)
	return m, byteArrayHeader, nil
}

func (m Mechanism) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if len(challenge) == 0 {
		return true, nil, nil
	}
	return false, nil, errors.New("invalid response")
}
