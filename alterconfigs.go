package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/alterconfigs"
)

// AlterConfigsRequest represents a request sent to a kafka broker to alter configs
type AlterConfigsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// List of resources to update
	Resources []AlterConfigRequestResource

	// When set to true, topics are not created but the configuration is
	// validated as if they were.
	ValidateOnly bool
}

type AlterConfigRequestResource struct {
	// Resource Type
	ResourceType int8

	// Resource Name
	ResourceName string

	// Configs is a List of configurations that need to update
	Configs []AlterConfigRequestConfig
}

type AlterConfigRequestConfig struct {
	// Configuration key name
	Name string

	// The value to set for the configuration key.
	Value string
}

// AlterConfigsResponse represents a response from a kafka broker to a alter config request.
type AlterConfigsResponse struct {
	// The amount of time that the broker throttled the request.
	//
	// This field will be zero if the kafka broker did no support the
	// AlterConfigs API in version 2 or above.
	Throttle time.Duration

	// Mapping of topic names to errors that occurred while attempting to create
	// the topics.
	//
	// The errors contain the kafka error code. Programs may use the standard
	// errors.Is function to test the error against kafka error codes.
	Errors map[string]error
}

// AlterConfigs sends a config altering request to a kafka broker and returns the
// response.
func (c *Client) AlterConfigs(ctx context.Context, req *AlterConfigsRequest) (*AlterConfigsResponse, error) {
	resources := make([]alterconfigs.RequestResources, len(req.Resources))

	for i, t := range req.Resources {
		configs := make([]alterconfigs.RequestConfig, len(t.Configs))
		for _, v := range t.Configs {
			config := alterconfigs.RequestConfig{
				Name:  v.Name,
				Value: v.Value,
			}
			configs = append(configs, config)
		}
		resources[i] = alterconfigs.RequestResources{
			ResourceType: t.ResourceType,
			ResourceName: t.ResourceName,
			Configs:      configs,
		}
	}

	m, err := c.roundTrip(ctx, req.Addr, &alterconfigs.Request{
		Resources:    resources,
		ValidateOnly: req.ValidateOnly,
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).AlterConfigs: %w", err)
	}

	res := m.(*alterconfigs.Response)
	ret := &AlterConfigsResponse{
		Throttle: makeDuration(res.ThrottleTimeMs),
		Errors:   make(map[string]error, len(res.Responses)),
	}

	for _, t := range res.Responses {
		ret.Errors[t.ResourceName] = makeError(t.ErrorCode, t.ErrorMessage)
	}

	return ret, nil
}
