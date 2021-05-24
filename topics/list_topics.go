package topics

import (
	"context"
	"regexp"

	kafkago "github.com/segmentio/kafka-go"
)

// ListTopics returns a slice of all the Topics
func ListTopics(ctx context.Context, client *kafkago.Client) (topics []kafkago.Topic, err error) {
	response, err := client.Metadata(ctx, &kafkago.MetadataRequest{
		Addr: client.Addr,
	})
	if err != nil {
		return nil, err
	}

	return response.Topics, nil
}

// ListTopicsRegex returns a slice of Topics that match a regex
func ListTopicsRegex(ctx context.Context, client *kafkago.Client, match regexp.Regexp) (topics []string, err error) {
	alltopics, err := ListTopics(ctx, client)
	if err != nil {
		return nil, err
	}

	for _, val := range alltopics {
		if match.Find([]byte(val.Name)) != nil {
			topics = append(topics, val.Name)
		}
	}
	return topics, nil
}
