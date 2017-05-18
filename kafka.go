package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type kafkaReader struct {
	client    sarama.Client
	partition int32
	topic     string
	events    chan Message
	errors    chan error
	asyncWait sync.WaitGroup
	spawned   bool
	cancel    context.CancelFunc
	offset    int64

	maxWaitMs uint
	minBytes  uint32
	maxBytes  uint32
}

type ReaderConfig struct {
	BrokerAddrs []string
	Topic       string
	Partition   uint

	// Kafka requests wait for `RequestMaxWaitTime` OR `RequestMinBytes`, but
	// always stops at `RequestMaxBytes`.
	RequestMaxWaitMs uint
	RequestMinBytes  uint32
	RequestMaxBytes  uint32
}

func NewReader(config ReaderConfig) (Reader, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_1_0

	client, err := sarama.NewClient(config.BrokerAddrs, conf)
	if err != nil {
		return nil, err
	}

	if config.RequestMaxBytes == 0 {
		config.RequestMaxBytes = 1000000
	}

	return &kafkaReader{
		events:    make(chan Message),
		errors:    make(chan error),
		spawned:   false,
		client:    client,
		offset:    0,
		cancel:    func() {},
		topic:     config.Topic,
		partition: int32(config.Partition),
		maxWaitMs: config.RequestMaxWaitMs,
		minBytes:  config.RequestMinBytes,
		maxBytes:  config.RequestMaxBytes,
	}, nil
}

// Start consuming from Kafka starting at the given `offset`. `Read` will return a sequential iterator that makes progress
// as its being read. Rewinding or needing to reset the offset will require calling `Read` again, returning a new iterator.
func (kafka *kafkaReader) Read(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case msg := <-kafka.events:
		return msg, nil
	case err := <-kafka.errors:
		return Message{}, err
	}
}

func (kafka *kafkaReader) closeAsync() {
	kafka.cancel()

	// Avoid blocking the async goroutine by emptying the channels.
	go func() {
		for _ = range kafka.events {
		}
		for _ = range kafka.errors {
		}
	}()

	kafka.asyncWait.Wait()

	close(kafka.events)
	close(kafka.errors)

	kafka.events = make(chan Message)
	kafka.errors = make(chan error)
}

func (kafka *kafkaReader) Seek(ctx context.Context, offset int64) (int64, error) {
	if kafka.spawned {
		kafka.closeAsync()
	}

	broker, err := kafka.client.Leader(kafka.topic, kafka.partition)
	if err != nil {
		return 0, err
	}

	offset, err = kafka.getOffset(broker, offset)
	if err != nil {
		return 0, err
	}

	atomic.StoreInt64(&kafka.offset, offset)

	asyncCtx, cancel := context.WithCancel(ctx)
	kafka.cancel = cancel

	kafka.asyncWait.Add(1)
	go kafka.fetchMessagesAsync(asyncCtx)
	kafka.spawned = true

	return offset, nil
}

func (kafka *kafkaReader) getOffset(broker *sarama.Broker, offset int64) (int64, error) {
	// An offset of -1/-2 means the partition has no offset state associated with it yet.
	//  -1 = Newest Offset
	//  -2 = Oldest Offset
	if offset == -1 || offset == -2 {
		request := &sarama.OffsetRequest{Version: 1}
		request.AddBlock(kafka.topic, kafka.partition, int64(offset), 1)

		offsetResponse, err := broker.GetAvailableOffsets(request)
		if err != nil {
			return offset, errors.Wrap(err, "failed to fetch available offsets")
		}

		block := offsetResponse.GetBlock(kafka.topic, kafka.partition)
		if block == nil {
			return offset, errors.Wrap(err, "fetching available offsets returned 0 blocks")
		}

		if block.Err != sarama.ErrNoError {
			return offset, errors.Wrap(err, "fetching available offsets failed")
		}

		if block.Offset != 0 {
			return block.Offset - 1, nil
		}

		return block.Offset, nil
	}

	return offset, nil
}

func (kafka *kafkaReader) Offset() int64 {
	return atomic.LoadInt64(&kafka.offset)
}

func (kafka *kafkaReader) fetchMessagesAsync(ctx context.Context) {
	defer kafka.asyncWait.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Find the broker that is the given partition's leader. Failure to fetch the leader is either
		// the result of an invalid topic/partition OR the broker/leader is unavailable. This can happen
		// due to a leader election happening (and thus the leader has changed).
		broker, err := kafka.client.Leader(kafka.topic, kafka.partition)
		if err != nil {
			kafka.errors <- errors.Wrap(err, "failed to find leader for topic/partition")
			continue
		}

		// The request will wait at most `maxWaitMs` (milliseconds) OR at most `minBytes`,
		// which ever happens first.
		request := sarama.FetchRequest{
			MaxWaitTime: int32(kafka.maxWaitMs),
			MinBytes:    int32(kafka.minBytes),
		}

		offset := atomic.LoadInt64(&kafka.offset)

		request.AddBlock(kafka.topic, kafka.partition, offset, int32(kafka.maxBytes))
		res, err := broker.Fetch(&request)
		if err != nil {
			kafka.errors <- errors.Wrap(err, "kafka reader failed to fetch a block")
			continue
		}

		partition := res.GetBlock(kafka.topic, kafka.partition)
		if partition == nil {
			kafka.errors <- fmt.Errorf("kafka topic/partition is invalid (topic: %s, partition: %d)", kafka.topic, kafka.partition)
			continue
		}

		// Possible errors: https://godoc.org/github.com/Shopify/sarama#KError
		if partition.Err != sarama.ErrNoError {
			kafka.errors <- errors.Wrap(partition.Err, "kafka block returned an error")
			continue
		}

		// Bump the current offset to the last offset in the message set. The new offset will
		// be used the next time we fetch a block from Kafka.
		//
		// This doesn't commit the offset in any way, it only allows the iterator to continue to
		// make progress.
		msgSet := partition.MsgSet.Messages

		if len(msgSet) == 0 {
			continue
		}

		// Bump the current offset to the last offset in the message set. The new offset will
		// be used the next time we fetch a block from Kafka.
		//
		// This doesn't commit the offset in any way, it only allows the iterator to continue to
		// make progress.
		offset = msgSet[len(msgSet)-1].Offset + 1
		atomic.StoreInt64(&kafka.offset, offset)

		for _, msg := range msgSet {
			// Give the message to the iterator. This will block if the consumer of the iterator
			// is blocking or not calling `.Next(..)`. This allows the Kafka reader to stay in-sync
			// with the consumer.
			kafka.events <- Message{
				Offset: msg.Offset,
				Key:    msg.Msg.Key,
				Value:  msg.Msg.Value,
			}
		}
	}
}

// Shutdown the Kafka client. The Kafka reader does not persist the offset
// when closing down and thus any iterator progress will be lost.
func (kafka *kafkaReader) Close() (err error) {
	kafka.closeAsync()
	return kafka.client.Close()
}
