package kafka

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
)

// Reader provides a high-level API for consuming messages from kafka.
//
// A Reader automatically manages reconnections to a kafka server, and
// blocking methods have context support for asynchronous cancellations.
type Reader struct {
	// immutable fields of the reader
	config ReaderConfig

	// communication channels between the parent reader and its subreaders
	msgs chan readerMessage

	// mutable fields of the reader (synchronized on the mutex)
	mutex   sync.Mutex
	join    sync.WaitGroup
	cancel  context.CancelFunc
	version int64
	offset  int64
	lag     int64
	closed  bool
}

// ReaderConfig is a configuration object used to create new instances of
// Reader.
type ReaderConfig struct {
	// The list of broker addresses used to connect to the kafka cluster.
	Brokers []string

	// The topic to read messages from.
	Topic string

	// The partition number to read messages from.
	Partition int

	// An dialer used to open connections to the kafka server. This field is
	// optional, if nil, the default dialer is used instead.
	Dialer *Dialer

	// The capacity of the internal message queue, defaults to 100 if none is
	// set.
	QueueCapacity int

	// Min and max number of bytes to fetch from kafka in each request.
	MinBytes int
	MaxBytes int

	// Maximum amount of time to wait for new data to come when fetching batches
	// of messages from kafka.
	MaxWait time.Duration
}

// NewReader creates and returns a new Reader configured with config.
func NewReader(config ReaderConfig) *Reader {
	if len(config.Brokers) == 0 {
		panic("cannot create a new kafka reader with an empty list of broker addresses")
	}

	if len(config.Topic) == 0 {
		panic("cannot create a new kafka reader with an empty topic")
	}

	if config.Partition < 0 || config.Partition >= math.MaxInt32 {
		panic(fmt.Sprintf("partition number out of bounds: %d", config.Partition))
	}

	if config.Dialer == nil {
		config.Dialer = DefaultDialer
	}

	if config.MinBytes > config.MaxBytes {
		panic(fmt.Sprintf("minimum batch size greater than the maximum (min = %d, max = %d)", config.MinBytes, config.MaxBytes))
	}

	if config.MinBytes < 0 {
		panic(fmt.Sprintf("invalid negative minimum batch size (min = %d)", config.MinBytes))
	}

	if config.MaxBytes < 0 {
		panic(fmt.Sprintf("invalid negative maximum batch size (max = %d)", config.MaxBytes))
	}

	if config.MaxBytes == 0 {
		config.MaxBytes = 10e6 // 10 MB
	}

	if config.MinBytes == 0 {
		config.MinBytes = config.MaxBytes
	}

	if config.MaxWait == 0 {
		config.MaxWait = 10 * time.Second
	}

	if config.QueueCapacity == 0 {
		config.QueueCapacity = 100
	}

	return &Reader{
		config: config,
		msgs:   make(chan readerMessage, config.QueueCapacity),
		cancel: func() {},
		offset: -1,
	}
}

// Config returns the reader's configuration.
func (r *Reader) Config() ReaderConfig {
	return r.config
}

// Close closes the stream, preventing the program from reading any more
// messages from it.
func (r *Reader) Close() error {
	r.mutex.Lock()
	closed := r.closed
	r.closed = true
	r.mutex.Unlock()

	r.cancel()
	r.join.Wait()

	if !closed {
		close(r.msgs)
	}

	return nil
}

// ReadMessage reads and return the next message from the r. The method call
// blocks until a message becomes available, or an error occurs. The program
// may also specify a context to asynchronously cancel the blocking operation.
func (r *Reader) ReadMessage(ctx context.Context) (Message, error) {
	for {
		r.mutex.Lock()

		if r.version == 0 {
			r.start()
		}

		version := r.version
		r.mutex.Unlock()

		select {
		case <-ctx.Done():
			return Message{}, ctx.Err()

		case m, ok := <-r.msgs:
			if !ok {
				return Message{}, io.ErrClosedPipe
			}

			if m.version >= version {
				r.mutex.Lock()

				switch {
				case m.error != nil:
				case version == r.version:
					r.offset = m.message.Offset + 1
				}

				r.lag = m.watermark - r.offset
				r.mutex.Unlock()
				return m.message, m.error
			}
		}
	}
}

// Offset returns the current offset of the reader.
func (r *Reader) Offset() int64 {
	r.mutex.Lock()
	offset := r.offset
	r.mutex.Unlock()
	return offset
}

// Lag returns the difference between the highest offset in a Kafka partition and the reader's
// current offset. This can be used as a queue-depth indicator.
func (r *Reader) Lag() int64 {
	r.mutex.Lock()
	lag := r.lag
	r.mutex.Unlock()
	return lag
}

// SetOffset changes the offset from which the next batch of messages will be
// read.
//
// The method fails with io.ErrClosedPipe if the reader has already been closed.
func (r *Reader) SetOffset(offset int64) error {
	var err error
	r.mutex.Lock()

	if r.closed {
		err = io.ErrClosedPipe
	} else if offset != r.offset {
		r.offset = offset

		if r.version != 0 {
			r.start()
		}
	}

	r.mutex.Unlock()
	return err
}

func (r *Reader) start() {
	ctx, cancel := context.WithCancel(context.Background())

	r.cancel() // always cancel the previous reader
	r.cancel = cancel
	r.version++

	r.join.Add(1)
	go (&reader{
		dialer:    r.config.Dialer,
		brokers:   r.config.Brokers,
		topic:     r.config.Topic,
		partition: r.config.Partition,
		minBytes:  r.config.MinBytes,
		maxBytes:  r.config.MaxBytes,
		maxWait:   r.config.MaxWait,
		version:   r.version,
		msgs:      r.msgs,
	}).run(ctx, r.offset, &r.join)
}

// A reader reads messages from kafka and produces them on its channels, it's
// used as an way to asynchronously fetch messages while the main program reads
// them using the high level reader API.
type reader struct {
	dialer    *Dialer
	brokers   []string
	topic     string
	partition int
	minBytes  int
	maxBytes  int
	maxWait   time.Duration
	version   int64
	msgs      chan<- readerMessage
}

type readerMessage struct {
	version   int64
	message   Message
	watermark int64
	error     error
}

func (r *reader) run(ctx context.Context, offset int64, join *sync.WaitGroup) {
	defer join.Done()
	// This is the reader's main loop, it only ends if the context is canceled
	// and will keep attempting to reader messages otherwise.
	//
	// Retrying indefinitely has the nice side effect of preventing Read calls
	// on the parent reader to block if connection to the kafka server fails,
	// the reader keeps reporting errors on the error channel which will then
	// be surfaced to the program.
	// If the reader wasn't retrying then the program would block indefinitely
	// on a Read call after reading the first error.
	for attempt := 0; true; attempt++ {
		if attempt != 0 {
			if !sleep(ctx, backoff(attempt, time.Second, time.Minute)) {
				return
			}
		}

		conn, start, err := r.initialize(ctx, offset)
		switch err {
		case nil:
		case OffsetOutOfRange:
			// This would happen if the requested offset is passed the last
			// offset on the partition leader. In that case we're just going
			// to retry later hoping that enough data has been produced.
		default:
			// Wait 4 attempts before reporting the first errors, this helps
			// mitigate situations where the kafka server is temporarily
			// unavailable.
			if attempt >= 3 {
				r.sendError(ctx, err)
			}
			continue
		}

		// Resetting the attempt counter ensures that if a failre occurs after
		// a successful initialization we don't keep increasing the backoff
		// timeout.
		attempt = 0

		// Now we're sure to have an absolute offset number, may anything happen
		// to the connection we know we'll want to restart from this offset.
		offset = start

		errcount := 0
	readLoop:
		for {
			if !sleep(ctx, backoff(errcount, 100*time.Millisecond, time.Second)) {
				conn.Close()
				return
			}

			switch offset, err = r.read(ctx, offset, conn); err {
			case nil, RequestTimedOut:
				// Timeout on the kafka side, this can be safely retried.
				errcount = 0
				continue
			case OffsetOutOfRange:
				// We may be reading past the last offset, will retry later.
			case context.Canceled:
				// Another reader has taken over, we can safely quit.
				conn.Close()
				return
			default:
				if _, ok := err.(Error); ok {
					r.sendError(ctx, err)
				} else {
					conn.Close()
					break readLoop
				}
			}

			errcount++
		}
	}
}

func (r *reader) initialize(ctx context.Context, offset int64) (conn *Conn, start int64, err error) {
	for i := 0; i != len(r.brokers) && conn == nil; i++ {
		var broker = r.brokers[i]
		var first int64

		if conn, err = r.dialer.DialLeader(ctx, "tcp", broker, r.topic, r.partition); err != nil {
			continue
		}

		// This deadline controls how long the offset negotiation may take.
		conn.SetDeadline(time.Now().Add(10 * time.Second))

		if first, err = conn.ReadFirstOffset(); err != nil {
			conn.Close()
			conn = nil
			break
		}

		// In case the reader was configured with an offset before the first
		// offset, skipping directly to the first offset.
		if offset < first {
			offset = first
		}

		if start, err = conn.Seek(offset, 1); err != nil {
			conn.Close()
			conn = nil
			break
		}
	}

	return
}

func (r *reader) read(ctx context.Context, offset int64, conn *Conn) (int64, error) {
	conn.SetReadDeadline(time.Now().Add(r.maxWait))
	batch := conn.ReadBatch(r.minBytes, r.maxBytes)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	var msg Message
	var err error

	for {
		if msg, err = batch.ReadMessage(); err != nil {
			err = batch.Close()
			break
		}

		if err = r.sendMessage(ctx, msg, batch.HighWaterMark()); err != nil {
			err = batch.Close()
			break
		}

		offset = msg.Offset + 1
	}

	return offset, err
}

func (r *reader) sendMessage(ctx context.Context, msg Message, watermark int64) error {
	select {
	case r.msgs <- readerMessage{version: r.version, message: msg, watermark: watermark}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *reader) sendError(ctx context.Context, err error) error {
	select {
	case r.msgs <- readerMessage{version: r.version, error: err}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
