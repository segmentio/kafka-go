package oauthbearer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go/sasl"
)

var ErrOAuthBearerAuth = errors.New("oauthbearer: authentication failed")

// Error represents an RFC 7628 authentication error from the server.
type Error struct {
	Status              string `json:"status"`
	Scope               string `json:"scope,omitempty"`
	OpenIDConfiguration string `json:"openid-configuration,omitempty"`
	Raw                 []byte `json:"-"`
}

func (e *Error) Error() string {
	if e.Status != "" && e.Scope != "" {
		return fmt.Sprintf("oauthbearer: status=%s scope=%s", e.Status, e.Scope)
	}
	if e.Status != "" {
		return fmt.Sprintf("oauthbearer: status=%s", e.Status)
	}
	if len(e.Raw) > 0 {
		return fmt.Sprintf("oauthbearer: %s", string(e.Raw))
	}
	return "oauthbearer: authentication failed"
}

func (e *Error) Unwrap() error             { return ErrOAuthBearerAuth }
func (e *Error) IsInvalidToken() bool      { return e.Status == "invalid_token" }
func (e *Error) IsInsufficientScope() bool { return e.Status == "insufficient_scope" }

// Mechanism implements the OAUTHBEARER SASL mechanism (RFC 7628).
type Mechanism struct {
	TokenFunc  func(ctx context.Context) (token string, err error)
	Extensions map[string]string
}

func (m *Mechanism) Name() string { return "OAUTHBEARER" }

func (m *Mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	if m.TokenFunc == nil {
		return nil, nil, errors.New("oauthbearer: TokenFunc is required")
	}
	token, err := m.TokenFunc(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("oauthbearer: failed to get token: %w", err)
	}
	if token == "" {
		return nil, nil, errors.New("oauthbearer: token cannot be empty")
	}
	// RFC 7628: n,,^Aauth=Bearer <token>^A^A
	response := "n,,\x01auth=Bearer " + token
	for k, v := range m.Extensions {
		response += "\x01" + k + "=" + v
	}
	response += "\x01\x01"
	return m, []byte(response), nil
}

func (m *Mechanism) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if len(challenge) == 0 {
		return true, nil, nil
	}
	return false, []byte{0x01}, parseError(challenge)
}

func parseError(challenge []byte) *Error {
	oauthErr := &Error{Raw: challenge}
	_ = json.Unmarshal(challenge, oauthErr) // ignore parse errors, Raw is fallback
	return oauthErr
}
