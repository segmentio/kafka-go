package kafka

import (
	"context"
	"testing"
)

func TestClientDescribeConfigs(t *testing.T) {

	client, shutdown := newLocalClient()
	defer shutdown()

	topic := makeTopic()
	createTopic(t, topic, 1)
	defer deleteTopic(t, topic)

	res, err := client.DescribeConfigs(context.Background(), &DescribeConfigsRequest{
		Resources: []DescribeConfigRequestResource{{
			ResourceType: ResourceTypeTopic,
			ResourceName: topic,
			ConfigNames:  []string{"max.message.bytes"},
		}},
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Client.DescribeConfigs() = %v", res)
}
