package oauthbearer

import (
	"context"
	"errors"
	"sort"

	"github.com/segmentio/kafka-go/sasl"
)

// Mechanism implements the OAUTHBEARER mechanism and passes the token.
type Mechanism struct {
	Zid        string
	Token      string
	Extensions map[string]string
}

func (Mechanism) Name() string {
	return "OAUTHBEARER"
}

func (m Mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	type kv struct {
		k string
		v string
	}
	kvs := make([]kv, 0, len(m.Extensions))
	for k, v := range m.Extensions {
		if len(k) == 0 {
			continue
		}
		kvs = append(kvs, kv{k, v})
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].k < kvs[j].k })

	gs2Header := "n,"
	if m.Zid != "" {
		gs2Header += "a=" + m.Zid
	}
	gs2Header += ","
	init := []byte(gs2Header + "\x01auth=Bearer ")
	init = append(init, m.Token...)
	init = append(init, '\x01')
	for _, kv := range kvs {
		init = append(init, kv.k...)
		init = append(init, '=')
		init = append(init, kv.v...)
		init = append(init, '\x01')
	}
	init = append(init, '\x01')

	return m, init, nil
}

func (m Mechanism) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if len(challenge) != 0 {
		return false, nil, errors.New("unexpected data in oauth response")
	}
	return true, nil, nil
}
