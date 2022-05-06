// Package topics is an experimental package that provides additional tooling
// around Kafka Topics. This package does not make any promises around
// backwards compatibility.
package topics

import (
	"context"
	"errors"
	"regexp"

	"github.com/segmentio/kafka-go"
)

// List returns a slice of all the Topics.
func List(ctx context.Context, client *kafka.Client) (topics []kafka.Topic, err error) {
	if client == nil {
		return nil, errors.New("client is required")
	}
	response, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr: client.Addr,
	})
	if err != nil {
		return nil, err
	}

	return response.Topics, nil
}

// ListRe returns a slice of Topics that match a regex.
func ListRe(ctx context.Context, cli *kafka.Client, re *regexp.Regexp) (topics []kafka.Topic, err error) {
	alltopics, err := List(ctx, cli)
	if err != nil {
		return nil, err
	}

	for _, val := range alltopics {
		if re.MatchString(val.Name) {
			topics = append(topics, val)
		}
	}
	return topics, nil
}
