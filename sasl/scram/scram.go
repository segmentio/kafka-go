package scram

import (
	"context"
	"crypto/sha512"
	"hash"

	"github.com/pkg/errors"
	"github.com/xdg/scram"
)

// HashFunction determines the hash function used by SCRAM to protect the user's
// credentials.
type HashFunction int

const (
	_ HashFunction = iota
	SHA256
	SHA512
)

func (a HashFunction) name() string {
	switch a {
	case SHA256:
		return "SCRAM-SHA-256"
	case SHA512:
		return "SCRAM-SHA-512"
	}
	return "invalid"
}

func (a HashFunction) hashGenerator() scram.HashGeneratorFcn {
	switch a {
	case SHA256:
		return scram.SHA256
	case SHA512:
		// for whatever reason, the scram package doesn't have a predefined
		// constant for 512, but we can roll our own.
		return scram.HashGeneratorFcn(func() hash.Hash {
			return sha512.New()
		})
	}
	return nil
}

type mechanism struct {
	hash   HashFunction
	client *scram.Client
	convo  *scram.ClientConversation
}

// Mechanism returns a new sasl.Mechanism that will use SCRAM with the provided
// hash function to securely transmit the provided credentials to Kafka.
//
// SCRAM-SHA-256 and SCRAM-SHA-512 were added to Kafka in 0.10.2.0.  These
// mechanisms will not work with older versions.
func Mechanism(hash HashFunction, username, password string) (*mechanism, error) {
	hashGen := hash.hashGenerator()
	if hashGen == nil {
		return nil, errors.New("invalid hash function")
	}

	client, err := hashGen.NewClient(username, password, "")
	if err != nil {
		return nil, err
	}

	return &mechanism{
		hash:   hash,
		client: client,
	}, nil
}

func (m *mechanism) Start(ctx context.Context) (string, []byte, error) {
	m.convo = m.client.NewConversation()
	str, err := m.convo.Step("")
	return m.hash.name(), []byte(str), err
}

func (m *mechanism) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	str, err := m.convo.Step(string(challenge))
	return m.convo.Done(), []byte(str), err
}
