package kafka

import "context"

type Offset uint64

type Message struct {
	Offset Offset
	Key    []byte
	Value  []byte
}

type MessageIter interface {
	Next(*Message) bool
	Close() error
}

type Reader interface {
	Read(context.Context, Offset) MessageIter
	Close() error
}
