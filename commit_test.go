package kafka

import "testing"

func TestMakeCommit(t *testing.T) {
	msg := Message{
		Topic:     "blah",
		Partition: 1,
		Offset:    2,
	}

	commit := makeCommit(msg)
	if commit.topic != msg.Topic {
		t.Errorf("bad topic: expected %v; got %v", msg.Topic, commit.topic)
	}
	if commit.partition != msg.Partition {
		t.Errorf("bad partition: expected %v; got %v", msg.Partition, commit.partition)
	}
	if commit.offset != msg.Offset+1 {
		t.Errorf("expected committed offset to be 1 greater than msg offset")
	}
}
