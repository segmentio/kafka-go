package kafka

import (
	"context"
	"testing"

	ktesting "github.com/apoorvag-mav/kafka-go/testing"
	"github.com/stretchr/testify/assert"
)

func TestClientDescribeConfigs(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		return
	}

	const (
		MaxMessageBytes      = "max.message.bytes"
		MaxMessageBytesValue = "200000"
	)

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	_, err := client.AlterConfigs(context.Background(), &AlterConfigsRequest{
		Resources: []AlterConfigRequestResource{{
			ResourceType: ResourceTypeTopic,
			ResourceName: topic,
			Configs: []AlterConfigRequestConfig{{
				Name:  MaxMessageBytes,
				Value: MaxMessageBytesValue,
			},
			},
		}},
	})

	if err != nil {
		t.Fatal(err)
	}

	describeResp, err := client.DescribeConfigs(context.Background(), &DescribeConfigsRequest{
		Resources: []DescribeConfigRequestResource{{
			ResourceType: ResourceTypeTopic,
			ResourceName: topic,
			ConfigNames:  []string{MaxMessageBytes},
		}},
	})

	maxMessageBytesValue := "0"
	for _, resource := range describeResp.Resources {
		if resource.ResourceType == int8(ResourceTypeTopic) && resource.ResourceName == topic {
			for _, entry := range resource.ConfigEntries {
				if entry.ConfigName == MaxMessageBytes {
					maxMessageBytesValue = entry.ConfigValue
				}
			}
		}
	}
	assert.Equal(t, maxMessageBytesValue, MaxMessageBytesValue)
}
