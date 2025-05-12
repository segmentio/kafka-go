package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"
)

// InitReaderGroup initializes a group of readers for the provided partitions
// when a partitions slice of the length 0 is passed the group is
// initialized with readers for all partitions of the provided topic
// this is not a ConsumerGroup in the sense of kafka. If you want to use ConsumerGroups have a look at NewReader
func InitReaderGroup(ctx context.Context, config ReaderConfig, partitions ...int) (*ReaderGroup, error) {
	if config.GroupID != "" {
		return nil, errors.New("setting GroupID is not allowed")
	}
	if config.Partition != 0 {
		return nil, errors.New("setting partition is not allowed")
	}
	if len(partitions) == 0 {
		if len(config.Brokers) == 0 {
			return nil, errors.New("missing brokers")
		}
		dialer := config.Dialer
		if dialer == nil {
			dialer = DefaultDialer
		}
		pp, err := dialer.LookupPartitions(
			ctx,
			"tcp",
			config.Brokers[0],
			config.Topic,
		)
		if err != nil {
			return nil, fmt.Errorf("looking up partitions: %w", err)
		}
		partitions = make([]int, len(pp))
		for i := range pp {
			partitions[i] = pp[i].ID
		}
	}

	group := ReaderGroup{
		readers: make([]*Readers, len(partitions)),
	}

	for i := range partitions {
		config.Partition = partitions[i]
		group.readers[i] = NewReader(config)
	}

	go g.startFetching(ctx)

	return &g, nil
}

type ReaderGroup struct {
	readers  []*Reader
	messages chan fetchMessageResponse
}

type fetchMessageResponse struct {
	msg Message
	err error
}

// Close closes the connections
func (g *ReaderGroup) Close() error {
	defer close(g.messages)

	var errg errgroup.Group

	for i := range g.readers {
		errg.Go(g.readers[i].Close)
	}

	return errg.Wait()
}

func (g *ReaderGroup) startFetching(ctx context.Context) {
	g.messages = make(chan fetchMessageResponse)

	var wg sync.WaitGroup
	wg.Add(len(g.readers))

	for i := range g.readers {
		go func(reader *Reader) {
			for {
				msg, err := reader.FetchMessage(ctx)
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					wg.Done()
					return
				}
				c.messages <- fetchMessageResponse{
					msg: msg,
					err: err,
				}
			}
		}(g.readers[i])
	}

	wg.Wait()
	close(g.messages)
}

func (g *ReaderGroup) FetchMessage(ctx context.Context) (Message, error) {
	select {
	case res, ok := <-g.messages:
		if !ok {
			return Message{}, io.EOF
		}
		return res.msg, res.err
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}

func (g *ReaderGroup) ReadLag(ctx context.Context) (int64, error) {
	var errg errgroup.Group

	// we are using the slice, because we are not writing concurrently
	// on multiple indices
	lags := make([]int64, len(g.readers))
	go func() {
		for i := range g.readers {
			// copy to have the value in the closure (i is incremented by the loop)
			n := i
			errg.Go(func() error {
				var err error
				lags[n], err = g.readers[n].ReadLag(ctx)
				if err != nil {
					return err
				}
				return nil
			})
		}
	}()

	err := errg.Wait()
	if err != nil {
		return 0, err
	}

	var totalLag int64
	for _, lag := range lags {
		totalLag += lag
	}

	return totalLag, nil
}
