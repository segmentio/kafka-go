package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

const (
	OffsetNewest int64 = -1
	OffsetOldest int64 = -2
)

type kafkaReader struct {
	client    sarama.Client
	partition int32
	topic     string

	// async task
	events    chan Message
	errors    chan error
	asyncWait sync.WaitGroup
	mu        sync.RWMutex
	// Determine if an async task has been spawned
	spawned bool
	// Cancel the async task
	cancel context.CancelFunc

	offset int64

	// kafka fetch configuration
	maxWaitTime time.Duration
	minBytes    int
	maxBytes    int
}

type ReaderConfig struct {
	BrokerAddrs []string
	Topic       string
	Partition   int

	// Kafka requests wait for `RequestMaxWaitTime` OR `RequestMinBytes`, but
	// always stops at `RequestMaxBytes`.
	RequestMaxWaitTime time.Duration
	RequestMinBytes    int
	RequestMaxBytes    int
}

// Create a new base Kafka reader given a topic and partition.
func NewReader(config ReaderConfig) (Reader, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_1_0

	client, err := sarama.NewClient(config.BrokerAddrs, conf)
	if err != nil {
		return nil, err
	}

	if config.Partition < 0 {
		return nil, errors.New("partitions cannot be negative.")
	}

	if config.RequestMinBytes < 0 {
		return nil, errors.New("minimum bytes for requests must be greater than 0")
	}

	if config.RequestMaxBytes < 0 {
		return nil, errors.New("maximum bytes for requests must be greater than 0")
	}

	// MaxBytes of 0 would make the task spin continuously without
	// actually returning data.
	if config.RequestMaxBytes == 0 {
		config.RequestMaxBytes = 1000000
	}

	return &kafkaReader{
		events:      make(chan Message),
		errors:      make(chan error),
		spawned:     false,
		client:      client,
		offset:      0,
		cancel:      func() {},
		topic:       config.Topic,
		partition:   int32(config.Partition),
		maxWaitTime: config.RequestMaxWaitTime,
		minBytes:    config.RequestMinBytes,
		maxBytes:    config.RequestMaxBytes,
	}, nil
}

// Read the next message from the underlying asynchronous task fetching from Kafka.
// `Read` will block until a message is ready to be read. The asynchronous task
// will also block until `Read` is called.
func (kafka *kafkaReader) Read(ctx context.Context) (Message, error) {
	kafka.mu.RLock()
	defer kafka.mu.RUnlock()

	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case msg := <-kafka.events:
		return msg, nil
	case err := <-kafka.errors:
		return Message{}, err
	}
}

// Cancel the asynchronous task, flush the events/errors channel to avoid blocking
// the goroutine and wait for the async task to close.
func (kafka *kafkaReader) closeAsync() {
	kafka.cancel()

	// Avoid blocking the async goroutine by emptying the channels.
	go func() {
		kafka.mu.RLock()
		defer kafka.mu.RUnlock()

		for _ = range kafka.events {
		}
		for _ = range kafka.errors {
		}
	}()

	kafka.asyncWait.Wait()

	close(kafka.events)
	close(kafka.errors)

	kafka.mu.Lock()
	defer kafka.mu.Unlock()
	// In-case `Seek` is called again and we need to these channels
	kafka.events = make(chan Message)
	kafka.errors = make(chan error)
}

// Given an offset spawn an asynchronous task that will read messages from Kafka. The offset
// can be a real Kafka offset or the special -1/-2.
//
// -1 = Newest Offset
// -2 = Oldest Offset
//
// If you want to start at the beginning of the partition, use `-2`.
// If you want to start at the end of the partition, use `-1`.
//
// Calling `Seek` again will cancel the previous task and create a new one.
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

	kafka.mu.RLock()
	defer kafka.mu.RUnlock()

	go kafka.fetchMessagesAsync(asyncCtx, kafka.events, kafka.errors)
	kafka.spawned = true

	return offset, nil
}

// Get the real offset from Kafka. If a non-negative offset is provided this method won't
// do anything and will return the same offset. If the offset provided is -1/-2 then it will
// make a call to Kafka to fetch the real offset.
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

// Return the current offset. This will return `0` if `Seek` hasn't been called.
func (kafka *kafkaReader) Offset() int64 {
	return atomic.LoadInt64(&kafka.offset)
}

// Internal asynchronous task to fetch messages from Kafka and send to an internal
// channel.
func (kafka *kafkaReader) fetchMessagesAsync(ctx context.Context, eventsCh chan<- Message, errorsCh chan<- error) {
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
			errorsCh <- errors.Wrap(err, "failed to find leader for topic/partition")
			continue
		}

		// The request will wait at most `maxWaitMs` (milliseconds) OR at most `minBytes`,
		// which ever happens first.
		request := sarama.FetchRequest{
			MaxWaitTime: int32(kafka.maxWaitTime.Seconds() / 1000),
			MinBytes:    int32(kafka.minBytes),
		}

		offset := atomic.LoadInt64(&kafka.offset)

		request.AddBlock(kafka.topic, kafka.partition, offset, int32(kafka.maxBytes))
		res, err := broker.Fetch(&request)
		if err != nil {
			errorsCh <- errors.Wrap(err, "kafka reader failed to fetch a block")
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		partition := res.GetBlock(kafka.topic, kafka.partition)
		if partition == nil {
			errorsCh <- fmt.Errorf("kafka topic/partition is invalid (topic: %s, partition: %d)", kafka.topic, kafka.partition)
			continue
		}

		// Possible errors: https://godoc.org/github.com/Shopify/sarama#KError
		if partition.Err != sarama.ErrNoError {
			errorsCh <- errors.Wrap(partition.Err, "kafka block returned an error")
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
			eventsCh <- Message{
				Offset: msg.Offset,
				Key:    msg.Msg.Key,
				Value:  msg.Msg.Value,
			}
		}
	}
}

// Shutdown the Kafka client.
func (kafka *kafkaReader) Close() (err error) {
	kafka.closeAsync()
	return kafka.client.Close()
}
