package kafka

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	firstOffset = -1
	lastOffset  = -2
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
	stop    context.CancelFunc
	version int64
	offset  int64
	lag     int64
	closed  bool

	// reader stats are all made of atomic values, no need for synchronization.
	once  uint32
	stctx context.Context
	stats readerStats
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

	// ReadLagInterval sets the frequency at which the reader lag is updated.
	// Setting this field to a negative value disables lag reporting.
	ReadLagInterval time.Duration

	// If not nil, specifies a logger used to report internal changes within the
	// reader.
	Logger *log.Logger

	// ErrorLogger is the logger used to report errors. If nil, the reader falls
	// back to using Logger instead.
	ErrorLogger *log.Logger
}

// ReaderStats is a data structure returned by a call to Reader.Stats that exposes
// details about the behavior of the reader.
type ReaderStats struct {
	Dials      int64 `metric:"kafka.reader.dial.count"      type:"counter"`
	Fetches    int64 `metric:"kafak.reader.fetch.count"     type:"counter"`
	Messages   int64 `metric:"kafka.reader.message.count"   type:"counter"`
	Bytes      int64 `metric:"kafka.reader.message.bytes"   type:"counter"`
	Rebalances int64 `metric:"kafka.reader.rebalance.count" type:"counter"`
	Timeouts   int64 `metric:"kafka.reader.timeout.count"   type:"counter"`
	Errors     int64 `metric:"kafka.reader.error.count"     type:"counter"`

	DialTime   DurationStats `metric:"kafka.reader.dial.seconds"`
	ReadTime   DurationStats `metric:"kafka.reader.read.seconds"`
	WaitTime   DurationStats `metric:"kafka.reader.wait.seconds"`
	FetchSize  SummaryStats  `metric:"kafka.reader.fetch.size"`
	FetchBytes SummaryStats  `metric:"kafka.reader.fetch.bytes"`

	Offset        int64         `metric:"kafka.reader.offset"          type:"gauge"`
	Lag           int64         `metric:"kafka.reader.lag"             type:"gauge"`
	MinBytes      int64         `metric:"kafka.reader.fetch_bytes.min" type:"gauge"`
	MaxBytes      int64         `metric:"kafka.reader.fetch_bytes.max" type:"gauge"`
	MaxWait       time.Duration `metric:"kafka.reader.fetch_wait.max"  type:"gauge"`
	QueueLength   int64         `metric:"kafka.reader.queue.length"    type:"gauge"`
	QueueCapacity int64         `metric:"kafka.reader.queue.capacity"  type:"gauge"`

	ClientID  string `tag:"client_id"`
	Topic     string `tag:"topic"`
	Partition string `tag:"partition"`
}

type readerStats struct {
	dials      counter
	fetches    counter
	messages   counter
	bytes      counter
	rebalances counter
	timeouts   counter
	errors     counter
	dialTime   summary
	readTime   summary
	waitTime   summary
	fetchSize  summary
	fetchBytes summary
	offset     gauge
	lag        gauge
	partition  string
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
		config.MaxBytes = 1e6 // 1 MB
	}

	if config.MinBytes == 0 {
		config.MinBytes = config.MaxBytes
	}

	if config.MaxWait == 0 {
		config.MaxWait = 10 * time.Second
	}

	if config.ReadLagInterval == 0 {
		config.ReadLagInterval = 1 * time.Minute
	}

	if config.QueueCapacity == 0 {
		config.QueueCapacity = 100
	}

	stctx, stop := context.WithCancel(context.Background())
	return &Reader{
		config: config,
		msgs:   make(chan readerMessage, config.QueueCapacity),
		cancel: func() {},
		stop:   stop,
		offset: firstOffset,
		stctx:  stctx,
		stats: readerStats{
			dialTime:   makeSummary(),
			readTime:   makeSummary(),
			waitTime:   makeSummary(),
			fetchSize:  makeSummary(),
			fetchBytes: makeSummary(),
			// Generate the string representation of the partition number only
			// once when the reader is created.
			partition: strconv.Itoa(config.Partition),
		},
	}
}

// Config returns the reader's configuration.
func (r *Reader) Config() ReaderConfig {
	return r.config
}

// Close closes the stream, preventing the program from reading any more
// messages from it.
func (r *Reader) Close() error {
	atomic.StoreUint32(&r.once, 1)

	r.mutex.Lock()
	closed := r.closed
	r.closed = true
	r.mutex.Unlock()

	r.cancel()
	r.stop()
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
	if r.config.ReadLagInterval > 0 && atomic.CompareAndSwapUint32(&r.once, 0, 1) {
		go r.readLag(r.stctx)
	}

	for {
		r.mutex.Lock()

		if !r.closed && r.version == 0 {
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
					r.lag = m.watermark - r.offset
				}

				r.mutex.Unlock()
				return m.message, m.error
			}
		}
	}
}

// ReadLag returns the current lag of the reader by fetching the last offset of
// the topic and partition and computing the difference between that value and
// the offset of the last message returned by ReadMessage.
//
// This method is intended to be used in cases where a program may be unable to
// call ReadMessage to update the value returned by Lag, but still needs to get
// an up to date estimation of how far behind the reader is. For example when
// the consumer is not ready to process the next message.
//
// The function returns a lag of zero when the reader's current offset is
// negative.
func (r *Reader) ReadLag(ctx context.Context) (lag int64, err error) {
	type offsets struct {
		first int64
		last  int64
	}

	offch := make(chan offsets, 1)
	errch := make(chan error, 1)

	go func() {
		var off offsets
		var err error

		for _, broker := range r.config.Brokers {
			var conn *Conn

			if conn, err = r.config.Dialer.DialLeader(ctx, "tcp", broker, r.config.Topic, r.config.Partition); err != nil {
				continue
			}

			deadline, _ := ctx.Deadline()
			conn.SetDeadline(deadline)

			off.first, off.last, err = conn.ReadOffsets()
			conn.Close()

			if err == nil {
				break
			}
		}

		if err != nil {
			errch <- err
		} else {
			offch <- off
		}
	}()

	select {
	case off := <-offch:
		switch cur := r.Offset(); {
		case cur == firstOffset:
			lag = off.last - off.first

		case cur == lastOffset:
			lag = 0

		default:
			lag = off.last - cur
		}
	case err = <-errch:
	case <-ctx.Done():
		err = ctx.Err()
	}

	return
}

// Offset returns the current offset of the reader.
func (r *Reader) Offset() int64 {
	r.mutex.Lock()
	offset := r.offset
	r.mutex.Unlock()
	r.withLogger(func(log *log.Logger) {
		log.Printf("looking up offset of kafka reader for partition %d of %s: %d", r.config.Partition, r.config.Topic, offset)
	})
	return offset
}

// Lag returns the lag of the last message returned by ReadMessage.
func (r *Reader) Lag() int64 {
	r.mutex.Lock()
	lag := r.lag
	r.mutex.Unlock()
	return lag
}

// SetOffset changes the offset from which the next batch of messages will be
// read.
//
// Setting the offset ot -1 means to seek to the first offset.
// Setting the offset to -2 means to seek to the last offset.
//
// The method fails with io.ErrClosedPipe if the reader has already been closed.
func (r *Reader) SetOffset(offset int64) error {
	var err error
	r.mutex.Lock()

	if r.closed {
		err = io.ErrClosedPipe
	} else if offset != r.offset {
		r.withLogger(func(log *log.Logger) {
			log.Printf("setting the offset of the kafka reader for partition %d of %s from %d to %d",
				r.config.Partition, r.config.Topic, r.offset, offset)
		})
		r.offset = offset

		if r.version != 0 {
			r.start()
		}
	}

	r.mutex.Unlock()
	return err
}

// Stats returns a snapshot of the reader stats since the last time the method
// was called, or since the reader was created if it is called for the first
// time.
//
// A typical use of this method is to spawn a goroutine that will periodically
// call Stats on a kafka reader and report the metrics to a stats collection
// system.
func (r *Reader) Stats() ReaderStats {
	return ReaderStats{
		Dials:         r.stats.dials.snapshot(),
		Fetches:       r.stats.fetches.snapshot(),
		Messages:      r.stats.messages.snapshot(),
		Bytes:         r.stats.bytes.snapshot(),
		Rebalances:    r.stats.rebalances.snapshot(),
		Timeouts:      r.stats.timeouts.snapshot(),
		Errors:        r.stats.errors.snapshot(),
		DialTime:      r.stats.dialTime.snapshotDuration(),
		ReadTime:      r.stats.readTime.snapshotDuration(),
		WaitTime:      r.stats.waitTime.snapshotDuration(),
		FetchSize:     r.stats.fetchSize.snapshot(),
		FetchBytes:    r.stats.fetchBytes.snapshot(),
		Offset:        r.stats.offset.snapshot(),
		Lag:           r.stats.lag.snapshot(),
		MinBytes:      int64(r.config.MinBytes),
		MaxBytes:      int64(r.config.MaxBytes),
		MaxWait:       r.config.MaxWait,
		QueueLength:   int64(len(r.msgs)),
		QueueCapacity: int64(cap(r.msgs)),
		ClientID:      r.config.Dialer.ClientID,
		Topic:         r.config.Topic,
		Partition:     r.stats.partition,
	}
}

func (r *Reader) withLogger(do func(*log.Logger)) {
	if r.config.Logger != nil {
		do(r.config.Logger)
	}
}

func (r *Reader) withErrorLogger(do func(*log.Logger)) {
	if r.config.ErrorLogger != nil {
		do(r.config.ErrorLogger)
	} else {
		r.withLogger(do)
	}
}

func (r *Reader) readLag(ctx context.Context) {
	ticker := time.NewTicker(r.config.ReadLagInterval)
	defer ticker.Stop()

	for {
		timeout, cancel := context.WithTimeout(ctx, r.config.ReadLagInterval/2)
		lag, err := r.ReadLag(timeout)
		cancel()

		if err != nil {
			r.stats.errors.observe(1)
			r.withErrorLogger(func(log *log.Logger) {
				log.Printf("kafka reader failed to read lag of partition %d of %s", r.config.Partition, r.config.Topic)
			})
		} else {
			r.stats.lag.observe(lag)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reader) start() {
	ctx, cancel := context.WithCancel(context.Background())

	r.cancel() // always cancel the previous reader
	r.cancel = cancel
	r.version++

	r.join.Add(1)
	go (&reader{
		dialer:      r.config.Dialer,
		logger:      r.config.Logger,
		errorLogger: r.config.ErrorLogger,
		brokers:     r.config.Brokers,
		topic:       r.config.Topic,
		partition:   r.config.Partition,
		minBytes:    r.config.MinBytes,
		maxBytes:    r.config.MaxBytes,
		maxWait:     r.config.MaxWait,
		version:     r.version,
		msgs:        r.msgs,
		stats:       &r.stats,
	}).run(ctx, r.offset, &r.join)
}

// A reader reads messages from kafka and produces them on its channels, it's
// used as an way to asynchronously fetch messages while the main program reads
// them using the high level reader API.
type reader struct {
	dialer      *Dialer
	logger      *log.Logger
	errorLogger *log.Logger
	brokers     []string
	topic       string
	partition   int
	minBytes    int
	maxBytes    int
	maxWait     time.Duration
	version     int64
	msgs        chan<- readerMessage
	stats       *readerStats
}

type readerMessage struct {
	version   int64
	message   Message
	watermark int64
	error     error
}

func (r *reader) run(ctx context.Context, offset int64, join *sync.WaitGroup) {
	defer join.Done()

	const backoffDelayMin = 100 * time.Millisecond
	const backoffDelayMax = 1 * time.Second

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
			if !sleep(ctx, backoff(attempt, backoffDelayMin, backoffDelayMax)) {
				return
			}
		}

		r.withLogger(func(log *log.Logger) {
			log.Printf("initializing kafka reader for partition %d of %s starting at offset %d", r.partition, r.topic, offset)
		})

		conn, start, err := r.initialize(ctx, offset)
		switch err {
		case nil:
		case OffsetOutOfRange:
			// This would happen if the requested offset is passed the last
			// offset on the partition leader. In that case we're just going
			// to retry later hoping that enough data has been produced.
			r.withErrorLogger(func(log *log.Logger) {
				log.Printf("error initializing the kafka reader for partition %d of %s: %s", r.partition, r.topic, OffsetOutOfRange)
			})
		default:
			// Wait 4 attempts before reporting the first errors, this helps
			// mitigate situations where the kafka server is temporarily
			// unavailable.
			if attempt >= 3 {
				r.sendError(ctx, err)
			} else {
				r.stats.errors.observe(1)
				r.withErrorLogger(func(log *log.Logger) {
					log.Printf("error initializing the kafka reader for partition %d of %s: %s", r.partition, r.topic, err)
				})
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
			if !sleep(ctx, backoff(errcount, backoffDelayMin, backoffDelayMax)) {
				conn.Close()
				return
			}

			switch offset, err = r.read(ctx, offset, conn); err {
			case nil:
				errcount = 0
			case NotLeaderForPartition:
				r.withErrorLogger(func(log *log.Logger) {
					log.Printf("failed to read from current broker for partition %d of %s at offset %d, not the leader", r.partition, r.topic, offset)
				})

				conn.Close()

				// The next call to .initialize will re-establish a connection to the proper
				// partition leader.
				r.stats.rebalances.observe(1)
				break readLoop
			case RequestTimedOut:
				// Timeout on the kafka side, this can be safely retried.
				errcount = 0
				r.withErrorLogger(func(log *log.Logger) {
					log.Printf("no messages received from kafka within the allocated time for partition %d of %s at offset %d", r.partition, r.topic, offset)
				})
				r.stats.timeouts.observe(1)
				continue
			case OffsetOutOfRange:
				// We may be reading past the last offset, will retry later.
				r.withErrorLogger(func(log *log.Logger) {
					log.Printf("the kafka reader is reading past the last offset for partition %d of %s at offset %d", r.partition, r.topic, offset)
				})
			case context.Canceled:
				// Another reader has taken over, we can safely quit.
				conn.Close()
				return
			default:
				if _, ok := err.(Error); ok {
					r.sendError(ctx, err)
				} else {
					r.withErrorLogger(func(log *log.Logger) {
						log.Printf("the kafka reader got an unknown error reading partition %d of %s at offset %d: %s", r.partition, r.topic, offset, err)
					})
					r.stats.errors.observe(1)
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
		var last int64

		t0 := time.Now()
		conn, err = r.dialer.DialLeader(ctx, "tcp", broker, r.topic, r.partition)
		t1 := time.Now()
		r.stats.dials.observe(1)
		r.stats.dialTime.observeDuration(t1.Sub(t0))

		if err != nil {
			continue
		}

		conn.SetDeadline(time.Now().Add(10 * time.Second))

		if first, last, err = conn.ReadOffsets(); err != nil {
			conn.Close()
			conn = nil
			break
		}

		switch {
		case offset == firstOffset:
			offset = first

		case offset == lastOffset:
			offset = last

		case offset < first:
			offset = first
		}

		r.withLogger(func(log *log.Logger) {
			log.Printf("the kafka reader for partition %d of %s is seeking to offset %d", r.partition, r.topic, offset)
		})

		if start, err = conn.Seek(offset, 1); err != nil {
			conn.Close()
			conn = nil
			break
		}

		conn.SetDeadline(time.Time{})
	}

	return
}

func (r *reader) read(ctx context.Context, offset int64, conn *Conn) (int64, error) {
	r.stats.fetches.observe(1)
	r.stats.offset.observe(offset)

	t0 := time.Now()
	conn.SetReadDeadline(t0.Add(r.maxWait))

	batch := conn.ReadBatch(r.minBytes, r.maxBytes)
	highWaterMark := batch.HighWaterMark()

	t1 := time.Now()
	r.stats.waitTime.observeDuration(t1.Sub(t0))

	var msg Message
	var err error
	var size int64
	var bytes int64

	const safetyTimeout = 10 * time.Second
	deadline := time.Now().Add(safetyTimeout)
	conn.SetReadDeadline(deadline)

	for {
		if now := time.Now(); deadline.Sub(now) < (safetyTimeout / 2) {
			deadline = now.Add(safetyTimeout)
			conn.SetReadDeadline(deadline)
		}

		if msg, err = batch.ReadMessage(); err != nil {
			err = batch.Close()
			break
		}

		n := int64(len(msg.Key) + len(msg.Value))
		r.stats.messages.observe(1)
		r.stats.bytes.observe(n)

		if err = r.sendMessage(ctx, msg, highWaterMark); err != nil {
			err = batch.Close()
			break
		}

		offset = msg.Offset + 1
		r.stats.offset.observe(offset)
		r.stats.lag.observe(highWaterMark - offset)

		size++
		bytes += n
	}

	conn.SetReadDeadline(time.Time{})

	t2 := time.Now()
	r.stats.readTime.observeDuration(t2.Sub(t1))
	r.stats.fetchSize.observe(size)
	r.stats.fetchBytes.observe(bytes)
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

func (r *reader) withLogger(do func(*log.Logger)) {
	if r.logger != nil {
		do(r.logger)
	}
}

func (r *reader) withErrorLogger(do func(*log.Logger)) {
	if r.errorLogger != nil {
		do(r.errorLogger)
	} else {
		r.withLogger(do)
	}
}
