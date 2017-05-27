package kafka

import "time"

// Message is a data structure representing kafka messages.
type Message struct {
	Offset int64
	Key    []byte
	Value  []byte
	Time   time.Time
}

type MessageReader struct {
}
