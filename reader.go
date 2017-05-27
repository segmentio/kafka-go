package kafka

import "context"

type Reader interface {
	Offset() int64
	Lag() int64
	Read(context.Context) (Message, error)
	Seek(context.Context, int64) (int64, error)
	Close() error
}
