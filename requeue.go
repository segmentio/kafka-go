package kafka

import "time"

// A requeue represents the instruction of publishing an update of the last
// offset read by a program for a topic and partition.
type requeue struct {
	topic     string
	partition int
	size      int
	attempt   int
	offset    int64
}

// makeRequeue builds a requeue value from a message, the resulting requeue takes
// its topic, partition, and offset from the message.
func makeRequeue(msg Message) requeue {
	return requeue{
		topic:     msg.Topic,
		partition: msg.Partition,
		size:      len(msg.Key) + len(msg.Value),
		attempt:   msg.Attempt + 1,
		offset:    msg.Offset + 1,
	}
}

// makeRequeues generates a slice of requeues from a list of messages, it extracts
// the topic, partition, and offset of each message and builds the corresponding
// requeue slice.
func makeRequeues(msgs ...Message) []requeue {
	requeues := make([]requeue, len(msgs))

	for i, m := range msgs {
		requeues[i] = makeRequeue(m)
	}

	return requeues
}

// requeueRequest is the data type exchanged between the RequeueMessages method
// and internals of the reader's implementation.
type requeueRequest struct {
	requeues []requeue
	time     time.Time
	errch    chan<- error
}
