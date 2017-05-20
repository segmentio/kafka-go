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

type ReaderConfig struct {
	Brokers   []string
	Topic     string
	Partition int

	// Kafka requests wait for `RequestMaxWaitTime` OR `RequestMinBytes`, but
	// always stops at `RequestMaxBytes`.
	RequestMaxWaitTime time.Duration
	RequestMinBytes    int
	RequestMaxBytes    int
}

type kafkaReader struct {
	client    sarama.Client
	partition int32
	topic     string

	// Channels used by the async task
	events chan Message
	errors chan error
	// Wait until the async task has finished. This is used to ensure that the task
	// won't be sending on the channels and that we can close them.
	asyncWait sync.WaitGroup
	// Determine if an async task has been spawned
	spawned bool
	// Cancel the async task
	cancel context.CancelFunc

	// The current offset that is synchronized via atomics.
	offset int64

	// kafka fetch configuration
	maxWaitTime time.Duration
	minBytes    int
	maxBytes    int
}

// Create a new Kafka reader given a topic and partition.
func NewReader(config ReaderConfig) (Reader, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_1_0

	client, err := sarama.NewClient(config.Brokers, conf)
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
		topic:     config.Topic,
		partition: int32(config.Partition),
		client:    client,
		offset:    0,

		// async task fields
		events: make(chan Message),
		errors: make(chan error),
		// Provide a noop context canceling function.
		cancel:  func() {},
		spawned: false,

		// Request-level parameters
		maxWaitTime: config.RequestMaxWaitTime,
		minBytes:    config.RequestMinBytes,
		maxBytes:    config.RequestMaxBytes,
	}, nil
}

// Read the next message from the underlying asynchronous task fetching from Kafka.
// `Read` will block until a message is ready to be read. The asynchronous task
// will also block until `Read` is called.
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

// Cancel the asynchronous task, flush the events/errors channel to avoid blocking
// the goroutine and wait for the async task to close.
func (kafka *kafkaReader) closeAsync() {
	kafka.cancel()

	// Reference the channels locally to avoid having to synchronize within
	// the goroutine.
	events := kafka.events
	errors := kafka.errors

	// Avoid blocking the async goroutine by emptying the channels.
	go func() {
		for _ = range events {
		}
		for _ = range errors {
		}
	}()

	// Wait for the async task to finish canceling. The channels cannot be closed
	// until the task has returned otherwise it may panic.
	kafka.asyncWait.Wait()

	close(events)
	close(errors)

	// Re-establish the channels for future uses.
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
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Closing the async task before another Seek operation will avoid the channels to have partial data
	// from a previous Seek operation.
	if kafka.spawned {
		kafka.closeAsync()
	}

	offset, err := kafka.getOffset(ctx, offset)
	if err != nil {
		return 0, err
	}

	atomic.StoreInt64(&kafka.offset, offset)

	asyncCtx, cancel := context.WithCancel(ctx)
	kafka.cancel = cancel

	kafka.asyncWait.Add(1)

	go kafka.fetchMessagesAsync(asyncCtx, kafka.events, kafka.errors)
	kafka.spawned = true

	return offset, nil
}

// Get the real offset from Kafka. If a non-negative offset is provided this method won't
// do anything and will return the same offset. If the offset provided is -1/-2 then it will
// make a call to Kafka to fetch the real offset.
func (kafka *kafkaReader) getOffset(ctx context.Context, offset int64) (int64, error) {
	// An offset of -1/-2 means the partition has no offset state associated with it yet.
	//  -1 = Newest Offset
	//  -2 = Oldest Offset
	if offset != -1 && offset != -2 {
		return offset, nil
	}

	errs := make(chan error)
	val := make(chan int64)

	broker, err := kafka.getLeader(ctx)
	if err != nil {
		return 0, err
	}

	go func() {
		request := &sarama.OffsetRequest{Version: 1}
		request.AddBlock(kafka.topic, kafka.partition, int64(offset), 1)

		offsetResponse, err := broker.GetAvailableOffsets(request)
		if err != nil {
			errs <- errors.Wrap(err, "failed to fetch available offsets")
			return
		}

		block := offsetResponse.GetBlock(kafka.topic, kafka.partition)
		if block == nil {
			errs <- errors.Wrap(err, "fetching available offsets returned 0 blocks")
			return
		}

		if block.Err != sarama.ErrNoError {
			errs <- errors.Wrap(err, "fetching available offsets failed")
			return
		}

		if block.Offset != 0 {
			val <- block.Offset - 1
		} else {
			val <- block.Offset
		}
	}()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case err := <-errs:
		return 0, err
	case res := <-val:
		return res, nil
	}
}

// Return the current offset. This will return `0` if `Seek` hasn't been called.
func (kafka *kafkaReader) Offset() int64 {
	return atomic.LoadInt64(&kafka.offset)
}

func (kafka *kafkaReader) getMessages(ctx context.Context, offset int64) (*sarama.FetchResponse, error) {
	errs := make(chan error)
	val := make(chan *sarama.FetchResponse)

	broker, err := kafka.getLeader(ctx)
	if err != nil {
		return nil, err
	}

	go func() {
		// The request will wait at most `maxWaitMs` (milliseconds) OR at most `minBytes`,
		// which ever happens first.
		request := sarama.FetchRequest{
			MaxWaitTime: int32(kafka.maxWaitTime.Seconds() / 1000),
			MinBytes:    int32(kafka.minBytes),
		}

		request.AddBlock(kafka.topic, kafka.partition, offset, int32(kafka.maxBytes))

		res, err := broker.Fetch(&request)
		if err != nil {
			errs <- errors.Wrap(err, "kafka reader failed to fetch a block")
		} else {
			val <- res
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errs:
		return nil, err
	case res := <-val:
		return res, nil
	}
}

// Find the broker that is the given partition's leader. Failure to fetch the leader is either
// the result of an invalid topic/partition OR the broker/leader is unavailable. This can happen
// due to a leader election happening (and thus the leader has changed).
func (kafka *kafkaReader) getLeader(ctx context.Context) (*sarama.Broker, error) {
	errs := make(chan error)
	val := make(chan *sarama.Broker)

	go func() {
		broker, err := kafka.client.Leader(kafka.topic, kafka.partition)
		if err != nil {
			errs <- errors.Wrap(err, "failed to find leader for topic/partition")
		} else {
			val <- broker
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errs:
		return nil, err
	case broker := <-val:
		return broker, nil
	}
}

// Internal asynchronous task to fetch messages from Kafka and send to an internal
// channel.
func (kafka *kafkaReader) fetchMessagesAsync(ctx context.Context, eventsCh chan<- Message, errorsCh chan<- error) {
	defer kafka.asyncWait.Done()

	for {
		offset := atomic.LoadInt64(&kafka.offset)
		res, err := kafka.getMessages(ctx, offset)
		if err != nil && err == context.Canceled {
			return
		} else if err != nil {
			errorsCh <- err
			continue
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

		// Kafka returns messages from a disk and may end up returning partial messages. The consumers need to prune them
		// and re-request them, if needed.
		if partition.MsgSet.PartialTrailingMessage {
			// Remove the trailing message. The next offset will fetch it in-full.
			msgSet = msgSet[:len(msgSet)-1]
		}

		// Bump the current offset to the last offset in the message set. The new offset will
		// be used the next time we fetch a block from Kafka.
		//
		// This doesn't commit the offset in any way, it only allows the iterator to continue to
		// make progress.
		//
		// If there was a trailing message this next offset will start there.
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

// Shutdown the async task and the Kafka client.
func (kafka *kafkaReader) Close() (err error) {
	kafka.closeAsync()
	return kafka.client.Close()
}
