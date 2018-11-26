package kafka

import (
	"reflect"
	"testing"
)

func TestOffsetStash(t *testing.T) {
	const topic = "topic"

	newMessage := func(partition int, offset int64) Message {
		return Message{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		}
	}

	tests := map[string]struct {
		Given    offsetStash
		Messages []Message
		Expected offsetStash
	}{
		"nil": {},
		"empty given, single message": {
			Given:    offsetStash{},
			Messages: []Message{newMessage(0, 0)},
			Expected: offsetStash{
				partition{topic, 0}: 1,
			},
		},
		"ignores earlier offsets": {
			Given: offsetStash{
				partition{topic, 0}: 2,
			},
			Messages: []Message{newMessage(0, 0)},
			Expected: offsetStash{
				partition{topic, 0}: 2,
			},
		},
		"uses latest offset": {
			Given: offsetStash{},
			Messages: []Message{
				newMessage(0, 2),
				newMessage(0, 3),
				newMessage(0, 1),
			},
			Expected: offsetStash{
				partition{topic, 0}: 4,
			},
		},
		"uses latest offset, across multiple topics": {
			Given: offsetStash{},
			Messages: []Message{
				newMessage(0, 2),
				newMessage(0, 3),
				newMessage(0, 1),
				newMessage(1, 5),
				newMessage(1, 6),
			},
			Expected: offsetStash{
				partition{topic, 0}: 4,
				partition{topic, 1}: 7,
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			test.Given.mergeCommits(makeCommits(test.Messages...))
			if !reflect.DeepEqual(test.Expected, test.Given) {
				t.Errorf("expected %v; got %v", test.Expected, test.Given)
			}
		})
	}
}
