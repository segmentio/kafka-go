package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Implements the `MessageIter`.
//
// The underlying Kafka reader sends messages to the `msgs`
// channel and errors to the `err` channel. The Kafka reader is responsible
// for closing those channels.
type kafkaIter struct {
	msgs   chan Message
	errs   chan error
	err    error
	ctx    context.Context
	cancel context.CancelFunc
}

func newKafkaIter(ctx context.Context, cancel context.CancelFunc, msgs chan Message, errs chan error) *kafkaIter {
	return &kafkaIter{
		msgs:   msgs,
		errs:   errs,
		err:    nil,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (iter *kafkaIter) Next(msg *Message) bool {
	if iter.err != nil {
		return false
	}

	select {
	case <-iter.ctx.Done():
		iter.err = appendError(iter.err, iter.ctx.Err())
		return false
	case err := <-iter.errs:
		iter.err = appendError(iter.err, err)
		return false
	case val, ok := <-iter.msgs:
		if !ok {
			return false
		}

		*msg = val
	}

	return true
}

func (iter *kafkaIter) Close() error {
	iter.cancel()
	// Read the remaining messages so that the underlying reader may
	// finish and return. Otherwise the goroutine will leak.
	for _ = range iter.msgs {
	}
	for _ = range iter.errs {
	}
	return iter.err
}

type kafkaReader struct {
	client    sarama.Client
	partition int32
	buffer    int
	topic     string

	maxWaitTime uint
	minBytes    uint32
	maxBytes    uint32
}

type ReaderConfig struct {
	BrokerAddrs []string
	// Size of the iterator channel. Setting it at 0 means the underlying consumer and the iterator are 100% in-sync.
	// The consumer will only make progress as the iterator does. Setting it >0 will allow the consumer to fetch data
	// potentially faster than the iterator can read.
	Buffer    int
	Topic     string
	Partition uint

	// Kafka requests wait for `RequestMaxWaitTime` OR `RequestMinBytes`, but
	// always stops at `RequestMaxBytes`.
	RequestMaxWaitTime uint
	RequestMinBytes    uint32
	RequestMaxBytes    uint32
}

func NewReader(config ReaderConfig) (Reader, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_0_0

	client, err := sarama.NewClient(config.BrokerAddrs, conf)
	if err != nil {
		return nil, err
	}

	return &kafkaReader{
		client:      client,
		topic:       config.Topic,
		partition:   int32(config.Partition),
		buffer:      config.Buffer,
		maxWaitTime: config.RequestMaxWaitTime,
		minBytes:    config.RequestMinBytes,
		maxBytes:    config.RequestMaxBytes,
	}, nil
}

// Start consuming from Kafka starting at the given `offset`. `Read` will return a sequential iterator that makes progress
// as its being read. Rewinding or needing to reset the offset will require calling `Read` again, returning a new iterator.
func (kafka *kafkaReader) Read(ctx context.Context, offset Offset) MessageIter {
	messagesCh := make(chan Message, kafka.buffer)
	errsCh := make(chan error, 1)

	// If the iterator is closed before the context is canceled it would block
	// indefinitely (to flush the msgs/errs channels). The iterator will now
	// cancel the context which will propagate to the `asyncFetch` goroutine.
	ctx, cancel := context.WithCancel(ctx)

	go kafka.asyncFetch(ctx, offset, messagesCh, errsCh)

	return newKafkaIter(ctx, cancel, messagesCh, errsCh)
}

// Asynchronously fetch blocks of messages from Kafka, sending each message to the underlying iterator. The async consumer will progress
// in-sync with the underlying's iterator's progression. If the iterator is not being consumed from or blocks, so does the async process.
//
// Offset management is up to the consumer of the iterator to implement. The offset is incremented by the async process as messages are
// being read from Kafka but does not persist the offset in any way.
func (kafka *kafkaReader) asyncFetch(ctx context.Context, offset Offset, messagesCh chan<- Message, errsCh chan<- error) {
	defer close(messagesCh)
	defer close(errsCh)

	for {
		select {
		default:
			break
		case <-ctx.Done():
			return
		}

		// Find the broker that is the given partition's leader. Failure to fetch the leader is either
		// the result of an invalid topic/partition OR the broker/leader is unavailable. This can happen
		// due to a leader election happening (and thus the leader has changed).
		broker, err := kafka.client.Leader(kafka.topic, kafka.partition)
		if err != nil {
			errsCh <- err
			continue
		}

		// The request will wait at most `maxWaitTime` (milliseconds) OR at most `minBytes`,
		// which ever happens first.
		request := sarama.FetchRequest{
			MaxWaitTime: int32(kafka.maxWaitTime),
			MinBytes:    int32(kafka.minBytes),
		}

		request.AddBlock(kafka.topic, kafka.partition, int64(offset), int32(kafka.maxBytes))
		res, err := broker.Fetch(&request)
		if err != nil {
			errsCh <- errors.Wrap(err, "kafka reader failed to fetch a block")
			continue
		}

		block, ok := res.Blocks[kafka.topic]
		if !ok {
			continue
		}

		// The only way Kafka does _not_ return a block is if the
		// partition is invalid.
		partition, ok := block[kafka.partition]
		if !ok {
			errsCh <- fmt.Errorf("kafka partition is invalid (partition: %d)", kafka.partition)
			continue
		}

		// Possible errors: https://godoc.org/github.com/Shopify/sarama#KError
		if partition.Err != sarama.ErrNoError {
			errsCh <- errors.Wrap(partition.Err, "kafka block returned an error")
			continue
		}

		// Bump the current offset to the last offset in the message set. The new offset will
		// be used the next time we fetch a block from Kafka.
		//
		// This doesn't commit the offset in any way, it only allows the iterator to continue to
		// make progress.
		msgSet := partition.MsgSet.Messages
		offset = Offset(msgSet[len(msgSet)-1].Offset)

		for _, msg := range msgSet {
			// Give the message to the iterator. This will block if the consumer of the iterator
			// is blocking or not calling `.Next(..)`. This allows the Kafka reader to stay in-sync
			// with the consumer.
			messagesCh <- Message{
				Offset: Offset(msg.Offset),
				Key:    msg.Msg.Key,
				Value:  msg.Msg.Value,
			}
		}
	}
}

// Shutdown the Kafka client. The Kafka reader does not persist the offset
// when closing down and thus any iterator progress will be lost.
func (kafka *kafkaReader) Close() (err error) {
	return kafka.client.Close()
}
