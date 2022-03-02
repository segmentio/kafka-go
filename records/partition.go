package records

import (
	"context"
	"io"

	"github.com/segmentio/kafka-go"
)

type Partition struct {
	Addr      string
	Topic     string
	Partition int
	Transport kafka.RoundTripper
}

func (p *Partition) Insert(ctx context.Context, records kafka.RecordReader) (int64, error) {

	return -1, nil
}

func (p *Partition) Lookup(ctx context.Context, key []byte) (io.ReadCloser, error) {

	return nil, nil
}
