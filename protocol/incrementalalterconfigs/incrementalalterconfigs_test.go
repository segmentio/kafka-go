package incrementalalterconfigs_test

import (
	"testing"

	"github.com/apoorvag-mav/kafka-go/protocol"
	"github.com/apoorvag-mav/kafka-go/protocol/incrementalalterconfigs"
)

const (
	resourceTypeTopic  int8 = 2
	resourceTypeBroker int8 = 4
)

func TestMetadataRequestBroker(t *testing.T) {
	req := &incrementalalterconfigs.Request{
		Resources: []incrementalalterconfigs.RequestResource{
			{
				ResourceType: resourceTypeBroker,
				ResourceName: "1",
				Configs: []incrementalalterconfigs.RequestConfig{
					{
						Name:  "test-name1",
						Value: "test-value1",
					},
				},
			},
			{
				ResourceType: resourceTypeBroker,
				ResourceName: "1",
				Configs: []incrementalalterconfigs.RequestConfig{
					{
						Name:  "test-name2",
						Value: "test-value2",
					},
				},
			},
			{
				ResourceType: resourceTypeTopic,
				ResourceName: "test-topic1",
				Configs: []incrementalalterconfigs.RequestConfig{
					{
						Name:  "test-name3",
						Value: "test-value3",
					},
				},
			},
			{
				ResourceType: resourceTypeTopic,
				ResourceName: "test-topic2",
				Configs: []incrementalalterconfigs.RequestConfig{
					{
						Name:  "test-name4",
						Value: "test-value4",
					},
				},
			},
		},
	}
	b, err := req.Broker(protocol.Cluster{
		Brokers: map[int32]protocol.Broker{
			0: {
				ID: 0,
			},
			1: {
				ID: 1,
			},
		},
	})
	if err != nil {
		t.Fatal(
			"Unexpected error getting request broker",
			"expected", nil,
			"got", err,
		)
	}
	if b.ID != 1 {
		t.Error(
			"Unexpected id returned for request broker",
			"expected", 1,
			"got", b.ID,
		)
	}

	req = &incrementalalterconfigs.Request{
		Resources: []incrementalalterconfigs.RequestResource{
			{
				ResourceType: resourceTypeBroker,
				ResourceName: "1",
				Configs: []incrementalalterconfigs.RequestConfig{
					{
						Name:  "test-name1",
						Value: "test-value1",
					},
				},
			},
			{
				ResourceType: resourceTypeBroker,
				ResourceName: "2",
				Configs: []incrementalalterconfigs.RequestConfig{
					{
						Name:  "test-name2",
						Value: "test-value2",
					},
				},
			},
		},
	}

	_, err = req.Broker(protocol.Cluster{
		Brokers: map[int32]protocol.Broker{
			0: {
				ID: 0,
			},
			1: {
				ID: 1,
			},
			2: {
				ID: 1,
			},
		},
	})
	if err == nil {
		t.Fatal(
			"Unexpected error getting request broker",
			"expected", "non-nil",
			"got", err,
		)
	}
}
