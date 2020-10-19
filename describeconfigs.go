package kafka

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/segmentio/kafka-go/protocol/describeconfigs"
)

// DescribeConfigsRequest represents a request sent to a kafka broker to describe configs
type DescribeConfigsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	// List of resources to update
	Resources []DescribeConfigRequestResource

	// required API version is no less than v1
	IncludeSynonyms bool

	// required API version is no less than v3
	IncludeDocumentation bool
}

type DescribeConfigRequestResource struct {
	// Resource Type
	ResourceType int8

	// Resource Name
	ResourceName string

	// Configs is a List of configurations that need to update
	ConfigNames []string
}

// DescribeConfigsResponse represents a response from a kafka broker to a describe config request.
type DescribeConfigsResponse struct {
	// The amount of time that the broker throttled the request.
	//
	// This field will be zero if the kafka broker did no support the
	// DescribeConfigs API in version 2 or above.
	Throttle time.Duration

	// Resources
	Resources []DescribeConfigResponseResource
}

// DescribeConfigResponseResource
type DescribeConfigResponseResource struct {
	// Resource Type
	ResourceType int8

	// Resource Name
	ResourceName string

	// ErrorCode
	ErrorCode int16

	// ErrorMessage
	ErrorMessage string

	// ConfigEntries
	ConfigEntries []DescribeConfigResponseConfigEntry
}

// DescribeConfigResponseConfigEntry
type DescribeConfigResponseConfigEntry struct {
	ConfigName          string
	ConfigValue         string
	ReadOnly            bool
	IsDefault           bool
	IsSensitive         bool
	ConfigSynonyms      []DescribeConfigResponseConfigSynonym
	ConfigType          int8
	ConfigDocumentation string
}

// DescribeConfigResponseConfigSynonym
type DescribeConfigResponseConfigSynonym struct {
	ConfigName   string
	ConfigValue  string
	ConfigSource int8
}

// DescribeConfigs sends a config altering request to a kafka broker and returns the
// response.
func (c *Client) DescribeConfigs(ctx context.Context, req *DescribeConfigsRequest) (*DescribeConfigsResponse, error) {
	resources := make([]describeconfigs.RequestResource, len(req.Resources))

	for i, t := range req.Resources {
		configNames := make([]string, len(t.ConfigNames))
		for _, v := range t.ConfigNames {
			configNames = append(configNames, v)
		}
		resources[i] = describeconfigs.RequestResource{
			ResourceType: t.ResourceType,
			ResourceName: t.ResourceName,
			ConfigNames:  configNames,
		}
	}

	m, err := c.roundTrip(ctx, req.Addr, &describeconfigs.Request{
		Resources:            resources,
		IncludeSynonyms:      req.IncludeSynonyms,
		IncludeDocumentation: req.IncludeDocumentation,
	})

	if err != nil {
		return nil, fmt.Errorf("kafka.(*Client).DescribeConfigs: %w", err)
	}

	res := m.(*describeconfigs.Response)
	ret := &DescribeConfigsResponse{
		Throttle:  makeDuration(res.ThrottleTimeMs),
		Resources: make([]DescribeConfigResponseResource, len(res.Resources)),
	}

	for _, t := range res.Resources {

		configEntries := make([]DescribeConfigResponseConfigEntry, len(t.ConfigEntries))
		for _, v := range t.ConfigEntries {

			configSynonyms := make([]DescribeConfigResponseConfigSynonym, len(v.ConfigSynonyms))
			for _, cs := range v.ConfigSynonyms {
				configSynonyms = append(configSynonyms, DescribeConfigResponseConfigSynonym{
					ConfigName:   cs.ConfigName,
					ConfigValue:  cs.ConfigValue,
					ConfigSource: cs.ConfigSource,
				})
			}

			configEntries = append(configEntries, DescribeConfigResponseConfigEntry{
				ConfigName:          v.ConfigName,
				ConfigValue:         v.ConfigValue,
				ReadOnly:            v.ReadOnly,
				IsDefault:           v.IsDefault,
				IsSensitive:         v.IsSensitive,
				ConfigSynonyms:      configSynonyms,
				ConfigType:          v.ConfigType,
				ConfigDocumentation: v.ConfigDocumentation,
			})
		}

		resource := DescribeConfigResponseResource{
			ResourceType:  t.ResourceType,
			ResourceName:  t.ResourceName,
			ErrorCode:     t.ErrorCode,
			ErrorMessage:  t.ErrorMessage,
			ConfigEntries: configEntries,
		}
		ret.Resources = append(ret.Resources, resource)
	}

	return ret, nil
}
