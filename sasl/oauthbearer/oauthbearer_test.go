package oauthbearer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMechanismName(t *testing.T) {
	m := &Mechanism{}
	assert.Equal(t, "OAUTHBEARER", m.Name())
}

func TestMechanismStart(t *testing.T) {
	tests := []struct {
		name       string
		tokenFunc  func(context.Context) (string, error)
		extensions map[string]string
		wantErr    string
		wantResp   string
	}{
		{
			name:      "nil TokenFunc",
			tokenFunc: nil,
			wantErr:   "TokenFunc is required",
		},
		{
			name:      "TokenFunc returns error",
			tokenFunc: func(context.Context) (string, error) { return "", errors.New("token error") },
			wantErr:   "failed to get token",
		},
		{
			name:      "empty token",
			tokenFunc: func(context.Context) (string, error) { return "", nil },
			wantErr:   "token cannot be empty",
		},
		{
			name:      "valid token",
			tokenFunc: func(context.Context) (string, error) { return "test-token", nil },
			wantResp:  "n,,\x01auth=Bearer test-token\x01\x01",
		},
		{
			name:       "token with extensions",
			tokenFunc:  func(context.Context) (string, error) { return "test-token", nil },
			extensions: map[string]string{"ext": "value"},
			wantResp:   "n,,\x01auth=Bearer test-token\x01ext=value\x01\x01",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Mechanism{TokenFunc: tt.tokenFunc, Extensions: tt.extensions}
			sm, response, err := m.Start(context.Background())

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, sm)
				assert.Nil(t, response)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, sm)
			assert.Equal(t, tt.wantResp, string(response))
		})
	}
}

func TestMechanismStartReturnsItself(t *testing.T) {
	m := &Mechanism{TokenFunc: func(context.Context) (string, error) { return "token", nil }}
	sm, _, err := m.Start(context.Background())
	require.NoError(t, err)
	assert.Same(t, m, sm)
}

func TestMechanismNextSuccess(t *testing.T) {
	m := &Mechanism{TokenFunc: func(context.Context) (string, error) { return "token", nil }}
	sm, _, err := m.Start(context.Background())
	require.NoError(t, err)

	done, response, err := sm.Next(context.Background(), nil)

	assert.True(t, done)
	assert.Nil(t, response)
	assert.NoError(t, err)
}

func TestMechanismNextEmptyChallenge(t *testing.T) {
	m := &Mechanism{TokenFunc: func(context.Context) (string, error) { return "token", nil }}
	sm, _, _ := m.Start(context.Background())

	done, response, err := sm.Next(context.Background(), []byte{})

	assert.True(t, done)
	assert.Nil(t, response)
	assert.NoError(t, err)
}

func TestMechanismNextError(t *testing.T) {
	m := &Mechanism{TokenFunc: func(context.Context) (string, error) { return "token", nil }}
	sm, _, _ := m.Start(context.Background())

	challenge := []byte(`{"status":"invalid_token","scope":"admin"}`)
	done, response, err := sm.Next(context.Background(), challenge)

	// Must return done=false and 0x01 dummy response per RFC 7628
	assert.False(t, done)
	assert.Equal(t, []byte{0x01}, response)
	require.Error(t, err)

	// Verify error type
	var oauthErr *Error
	require.True(t, errors.As(err, &oauthErr))
	assert.Equal(t, "invalid_token", oauthErr.Status)
	assert.Equal(t, "admin", oauthErr.Scope)
}

func TestErrorsIs(t *testing.T) {
	m := &Mechanism{TokenFunc: func(context.Context) (string, error) { return "token", nil }}
	sm, _, _ := m.Start(context.Background())

	_, _, err := sm.Next(context.Background(), []byte(`{"status":"invalid_token"}`))

	assert.True(t, errors.Is(err, ErrOAuthBearerAuth))
}

func TestErrorsAs(t *testing.T) {
	m := &Mechanism{TokenFunc: func(context.Context) (string, error) { return "token", nil }}
	sm, _, _ := m.Start(context.Background())

	_, _, err := sm.Next(context.Background(), []byte(`{"status":"insufficient_scope","scope":"write"}`))

	var oauthErr *Error
	require.True(t, errors.As(err, &oauthErr))
	assert.Equal(t, "insufficient_scope", oauthErr.Status)
	assert.Equal(t, "write", oauthErr.Scope)
	assert.True(t, oauthErr.IsInsufficientScope())
	assert.False(t, oauthErr.IsInvalidToken())
}

func TestParseError(t *testing.T) {
	tests := []struct {
		name       string
		challenge  []byte
		wantStatus string
		wantScope  string
		wantOpenID string
	}{
		{
			name:       "valid JSON with all fields",
			challenge:  []byte(`{"status":"invalid_token","scope":"read","openid-configuration":"https://example.com/.well-known"}`),
			wantStatus: "invalid_token",
			wantScope:  "read",
			wantOpenID: "https://example.com/.well-known",
		},
		{
			name:       "status only",
			challenge:  []byte(`{"status":"insufficient_scope"}`),
			wantStatus: "insufficient_scope",
		},
		{
			name:      "invalid JSON",
			challenge: []byte("not json at all"),
		},
		{
			name:      "empty object",
			challenge: []byte(`{}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseError(tt.challenge)

			assert.Equal(t, tt.wantStatus, err.Status)
			assert.Equal(t, tt.wantScope, err.Scope)
			assert.Equal(t, tt.wantOpenID, err.OpenIDConfiguration)
			assert.Equal(t, tt.challenge, err.Raw)
		})
	}
}

func TestErrorMessage(t *testing.T) {
	tests := []struct {
		name    string
		err     *Error
		wantMsg string
	}{
		{
			name:    "status and scope",
			err:     &Error{Status: "invalid_token", Scope: "admin"},
			wantMsg: "oauthbearer: status=invalid_token scope=admin",
		},
		{
			name:    "status only",
			err:     &Error{Status: "insufficient_scope"},
			wantMsg: "oauthbearer: status=insufficient_scope",
		},
		{
			name:    "raw fallback",
			err:     &Error{Raw: []byte("server error message")},
			wantMsg: "oauthbearer: server error message",
		},
		{
			name:    "empty error",
			err:     &Error{},
			wantMsg: "oauthbearer: authentication failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMsg, tt.err.Error())
		})
	}
}

func TestTokenFuncCalledOnEachStart(t *testing.T) {
	callCount := 0
	m := &Mechanism{
		TokenFunc: func(context.Context) (string, error) {
			callCount++
			return "token", nil
		},
	}

	_, _, _ = m.Start(context.Background())
	_, _, _ = m.Start(context.Background())
	_, _, _ = m.Start(context.Background())

	assert.Equal(t, 3, callCount)
}

func TestTokenFuncReceivesContext(t *testing.T) {
	type ctxKey struct{}
	expectedValue := "test-value"

	m := &Mechanism{
		TokenFunc: func(ctx context.Context) (string, error) {
			val := ctx.Value(ctxKey{})
			if val != expectedValue {
				return "", errors.New("context value not passed")
			}
			return "token", nil
		},
	}

	ctx := context.WithValue(context.Background(), ctxKey{}, expectedValue)
	_, _, err := m.Start(ctx)

	assert.NoError(t, err)
}

func TestIsInvalidToken(t *testing.T) {
	err := &Error{Status: "invalid_token"}
	assert.True(t, err.IsInvalidToken())
	assert.False(t, err.IsInsufficientScope())
}

func TestIsInsufficientScope(t *testing.T) {
	err := &Error{Status: "insufficient_scope"}
	assert.True(t, err.IsInsufficientScope())
	assert.False(t, err.IsInvalidToken())
}

func TestMechanismImplementsSASLInterfaces(t *testing.T) {
	m := &Mechanism{TokenFunc: func(context.Context) (string, error) { return "token", nil }}

	// Verify Mechanism returns itself as StateMachine
	sm, _, err := m.Start(context.Background())
	require.NoError(t, err)

	// StateMachine should work correctly
	done, _, err := sm.Next(context.Background(), nil)
	assert.True(t, done)
	assert.NoError(t, err)
}

func TestExtensionsOrder(t *testing.T) {
	// Extensions use map iteration, so we just verify they're included
	m := &Mechanism{
		TokenFunc:  func(context.Context) (string, error) { return "token", nil },
		Extensions: map[string]string{"key1": "val1", "key2": "val2"},
	}

	_, response, err := m.Start(context.Background())
	require.NoError(t, err)

	respStr := string(response)
	assert.True(t, strings.HasPrefix(respStr, "n,,\x01auth=Bearer token"))
	assert.True(t, strings.HasSuffix(respStr, "\x01\x01"))
	assert.Contains(t, respStr, "\x01key1=val1")
	assert.Contains(t, respStr, "\x01key2=val2")
}
