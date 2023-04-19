package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/alterclientquotas"
)

// AlterClientQuotasRequest represents a request sent to a kafka broker to add
// alter client quotas.
type AlterClientQuotasRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// List of client quotas entries to alter.
	Entries []AlterClientQuotaEntry

	// Whether the alteration should be validated, but not performed.
	ValidateOnly bool
}

type AlterClientQuotaEntry struct {
	Entities []AlterClientQuotaEntity
	Ops      []AlterClientQuotaOps
}

type AlterClientQuotaEntity struct {
	EntityType string
	EntityName string
}

type AlterClientQuotaOps struct {
	Key    string
	Value  float64
	Remove bool
}

type AlterClientQuotaResponseQuotas struct {
	ErrorCode    int16
	ErrorMessage string
	Entities     []AlterClientQuotaEntity
}

// AlterClientQuotasResponse represents a response from a kafka broker to an alter client
// quotas request.
type AlterClientQuotasResponse struct {
	// The amount of time that the broker throttled the request.
	Throttle time.Duration

	// List of altered client quotas responses.
	Entries []AlterClientQuotaResponseQuotas
}

// AlterClientQuotas sends client quotas alteration request to a kafka broker and returns
// the response.
func (c *Client) AlterClientQuotas(ctx context.Context, req *AlterClientQuotasRequest) (*AlterClientQuotasResponse, error) {
	entries := make([]alterclientquotas.Entry, len(req.Entries))

	for entryIdx, entry := range req.Entries {
		entities := make([]alterclientquotas.Entity, len(entry.Entities))
		for entityIdx, entity := range entry.Entities {
			entities[entityIdx] = alterclientquotas.Entity{
				EntityType: entity.EntityType,
				EntityName: entity.EntityName,
			}
		}

		ops := make([]alterclientquotas.Ops, len(entry.Ops))
		for opsIdx, op := range entry.Ops {
			ops[opsIdx] = alterclientquotas.Ops{
				Key:    op.Key,
				Value:  op.Value,
				Remove: op.Remove,
			}
		}

		entries[entryIdx] = alterclientquotas.Entry{
			Entities: entities,
			Ops:      ops,
		}
	}

	m, err := c.roundTrip(ctx, req.Addr, &alterclientquotas.Request{
		Entries:      entries,
		ValidateOnly: req.ValidateOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).AlterClientQuotas: %w", err)
	}

	res := m.(*alterclientquotas.Response)
	responseEntries := make([]AlterClientQuotaResponseQuotas, len(res.Results))

	for responseEntryIdx, responseEntry := range res.Results {
		responseEntities := make([]AlterClientQuotaEntity, len(responseEntry.Entities))
		for responseEntityIdx, responseEntity := range responseEntry.Entities {
			responseEntities[responseEntityIdx] = AlterClientQuotaEntity{
				EntityType: responseEntity.EntityType,
				EntityName: responseEntity.EntityName,
			}
		}

		responseEntries[responseEntryIdx] = AlterClientQuotaResponseQuotas{
			ErrorCode:    responseEntry.ErrorCode,
			ErrorMessage: responseEntry.ErrorMessage,
			Entities:     responseEntities,
		}
	}
	ret := &AlterClientQuotasResponse{
		Throttle: makeDuration(res.ThrottleTimeMs),
		Entries:  responseEntries,
	}

	return ret, nil
}
