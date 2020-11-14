package kafka

import (
	"context"
	"fmt"
	"net"

	"github.com/segmentio/kafka-go/protocol/incrementalalterconfigs"
)

type AlterBrokerConfigsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	BrokerID      int
	ConfigEntries []ConfigEntry
}

type AlterBrokerConfigsResponse struct {
	ErrorCode    int16
	ErrorMessage string
}

type AlterTopicConfigsRequest struct {
	// Address of the kafka broker to send the request to.
	Addr net.Addr

	Topic         string
	ConfigEntries []ConfigEntry
}

type AlterTopicConfigsResponse struct {
	ErrorCode    int16
	ErrorMessage string
}

func (c *Client) AlterBrokerConfigs(
	ctx context.Context,
	req AlterBrokerConfigsRequest,
) (*AlterBrokerConfigsResponse, error) {
	configs := configEntriesToConfigs(req.ConfigEntries)
	apiReq := &incrementalalterconfigs.Request{
		Resources: []incrementalalterconfigs.RequestResource{
			{
				ResourceType: incrementalalterconfigs.ResourceTypeBroker,
				ResourceName: fmt.Sprintf("%d", req.BrokerID),
				Configs:      configs,
			},
		},
	}

	protocolResp, err := c.roundTrip(
		ctx,
		req.Addr,
		apiReq,
	)
	if err != nil {
		return nil, err
	}
	apiResp := protocolResp.(*incrementalalterconfigs.Response)

	if len(apiResp.Responses) == 0 {
		return nil, fmt.Errorf("Empty response")
	}

	return &AlterBrokerConfigsResponse{
		ErrorCode:    apiResp.Responses[0].ErrorCode,
		ErrorMessage: apiResp.Responses[0].ErrorMessage,
	}, nil
}

func (c *Client) AlterTopicConfigs(
	ctx context.Context,
	req AlterTopicConfigsRequest,
) (*AlterTopicConfigsResponse, error) {
	configs := configEntriesToConfigs(req.ConfigEntries)
	apiReq := &incrementalalterconfigs.Request{
		Resources: []incrementalalterconfigs.RequestResource{
			{
				ResourceType: incrementalalterconfigs.ResourceTypeBroker,
				ResourceName: req.Topic,
				Configs:      configs,
			},
		},
	}

	protocolResp, err := c.roundTrip(
		ctx,
		req.Addr,
		apiReq,
	)
	if err != nil {
		return nil, err
	}
	apiResp := protocolResp.(*incrementalalterconfigs.Response)

	if len(apiResp.Responses) == 0 {
		return nil, fmt.Errorf("Empty response")
	}

	return &AlterTopicConfigsResponse{
		ErrorCode:    apiResp.Responses[0].ErrorCode,
		ErrorMessage: apiResp.Responses[0].ErrorMessage,
	}, nil
}

func configEntriesToConfigs(entries []ConfigEntry) []incrementalalterconfigs.RequestConfig {
	configs := []incrementalalterconfigs.RequestConfig{}
	for _, entry := range entries {
		if entry.ConfigValue != "" {
			configs = append(
				configs,
				incrementalalterconfigs.RequestConfig{
					Name:            entry.ConfigName,
					Value:           entry.ConfigValue,
					ConfigOperation: incrementalalterconfigs.OpTypeSet,
				},
			)
		} else {
			configs = append(
				configs,
				incrementalalterconfigs.RequestConfig{
					Name:            entry.ConfigName,
					ConfigOperation: incrementalalterconfigs.OpTypeDelete,
				},
			)
		}
	}

	return configs
}
