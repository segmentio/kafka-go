package kafka

import (
	"context"
	"io"
	"sort"
	"sync"
	"time"
)

type Writer struct {
	config WriterConfig

	mutex  sync.RWMutex
	closed bool

	join sync.WaitGroup
	msgs chan writerMessage
	done chan struct{}
}

type WriterConfig struct {
	Brokers []string

	Topic string

	Dialer *Dialer

	Balancer Balancer

	Attempts int

	Capacity int

	BatchSize int

	BatchTimeout time.Duration

	ReadTimeout time.Duration

	WriteTimeout time.Duration

	RebalanceInterval time.Duration
}

func NewWriter(config WriterConfig) *Writer {
	if len(config.Brokers) == 0 {
		panic("cannot create a kafka writer with an empty list of brokers")
	}

	if len(config.Topic) == 0 {
		panic("cannot create a kafak writer with an empty topic")
	}

	if config.Dialer == nil {
		config.Dialer = DefaultDialer
	}

	if config.Balancer == nil {
		config.Balancer = &RoundRobin{}
	}

	if config.Attempts == 0 {
		config.Attempts = 10
	}

	if config.Capacity == 0 {
		config.Capacity = 100
	}

	if config.BatchSize == 0 {
		config.BatchSize = 100
	}

	if config.BatchTimeout == 0 {
		config.BatchTimeout = 1 * time.Second
	}

	if config.ReadTimeout == 0 {
		config.ReadTimeout = 10 * time.Second
	}

	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}

	if config.RebalanceInterval == 0 {
		config.RebalanceInterval = 15 * time.Second
	}

	w := &Writer{
		config: config,
		msgs:   make(chan writerMessage, config.Capacity),
		done:   make(chan struct{}),
	}

	w.join.Add(1)
	go w.run()
	return w
}

func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}

	var res = make(chan error, 1)
	var err error

	for attempt := 0; attempt != w.config.Attempts; attempt++ {
		w.mutex.RLock()

		if w.closed {
			w.mutex.RUnlock()
			return io.ErrClosedPipe
		}

		for _, msg := range msgs {
			select {
			case w.msgs <- writerMessage{
				msg: msg,
				res: res,
			}:
			case <-ctx.Done():
				w.mutex.RUnlock()
				return ctx.Err()
			}
		}

		w.mutex.RUnlock()

		var retry []Message
		for i := 0; i != len(msgs); i++ {
			select {
			case e := <-res:
				if e != nil {
					if we, ok := e.(*writerError); ok {
						retry, err = append(retry, we.msgs...), we.err
					} else {
						err = e
					}
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if msgs = retry; len(msgs) == 0 {
			break
		}

		timer := time.NewTimer(backoff(attempt+1, 100*time.Millisecond, 1*time.Second))
		select {
		case <-timer.C:
			err = nil
		case <-ctx.Done():
			err = ctx.Err()
		case <-w.done:
			err = io.ErrClosedPipe
		}
		timer.Stop()

		if err != nil {
			break
		}
	}

	return err
}

func (w *Writer) Close() (err error) {
	w.mutex.Lock()

	if !w.closed {
		w.closed = true
		close(w.msgs)
		close(w.done)
	}

	w.mutex.Unlock()
	w.join.Wait()
	return
}

func (w *Writer) run() {
	defer w.join.Done()

	ticker := time.NewTicker(w.config.RebalanceInterval)
	defer ticker.Stop()

	var rebalance = true
	var writers = make(map[int]*writer)
	var partitions []int
	var err error

	for {
		if rebalance {
			rebalance = false

			var newPartitions []int
			var oldPartitions = partitions

			if newPartitions, err = w.partitions(); err == nil {
				for _, partition := range diffp(oldPartitions, newPartitions) {
					w.close(writers[partition])
					delete(writers, partition)
				}

				for _, partition := range diffp(newPartitions, oldPartitions) {
					writers[partition] = w.open(partition)
				}

				partitions = newPartitions
			}
		}

		select {
		case wm, ok := <-w.msgs:
			if !ok {
				for _, writer := range writers {
					w.close(writer)
				}
				return
			}

			if len(partitions) != 0 {
				writers[w.config.Balancer.Balance(wm.msg.Key, partitions...)].msgs <- wm
			} else {
				select {
				default:
				case wm.res <- err:
				}
			}

		case <-ticker.C:
			rebalance = true
		}
	}
}

func (w *Writer) partitions() (partitions []int, err error) {
	for _, broker := range w.config.Brokers {
		var conn *Conn
		var plist []Partition

		ctx, cancel := context.WithTimeout(context.Background(), w.config.ReadTimeout)

		if conn, err = w.config.Dialer.DialContext(ctx, "tcp", broker); err != nil {
			cancel()
			continue
		}

		deadline, _ := ctx.Deadline()
		cancel()
		conn.SetReadDeadline(deadline)

		plist, err = conn.ReadPartitions(w.config.Topic)
		conn.Close()

		if err == nil {
			partitions = make([]int, len(plist))
			for i, p := range plist {
				partitions[i] = p.ID
			}
			break
		}
	}

	sort.Ints(partitions)
	return
}

func (w *Writer) open(partition int) (writer *writer) {
	writer = newWriter(partition, w.config)
	w.join.Add(1)

	go func() {
		defer w.join.Done()
		writer.run()
	}()

	return writer
}

func (w *Writer) close(writer *writer) {
	writer.close()
	w.join.Add(1)

	go func() {
		defer w.join.Done()
		w.mutex.RLock()

		if w.closed {
			for wm := range writer.msgs {
				select {
				case wm.res <- io.ErrClosedPipe:
				default:
				}
			}
		} else {
			for wm := range writer.msgs {
				w.msgs <- wm
			}
		}

		w.mutex.RUnlock()
	}()
}

func diffp(new []int, old []int) (diff []int) {
	for _, p := range new {
		if i := sort.SearchInts(old, p); i == len(old) || old[i] != p {
			diff = append(diff, p)
		}
	}
	return
}

type writer struct {
	brokers      []string
	topic        string
	partition    int
	batchSize    int
	batchTimeout time.Duration
	writeTimeout time.Duration
	dialer       *Dialer
	msgs         chan writerMessage
}

func newWriter(partition int, config WriterConfig) *writer {
	return &writer{
		brokers:      config.Brokers,
		topic:        config.Topic,
		partition:    partition,
		batchSize:    config.BatchSize,
		batchTimeout: config.BatchTimeout,
		writeTimeout: config.WriteTimeout,
		dialer:       config.Dialer,
		msgs:         make(chan writerMessage, config.Capacity),
	}
}

func (w *writer) close() {
	close(w.msgs)
}

func (w *writer) run() {
	ticker := time.NewTicker(w.batchTimeout / 10)
	defer ticker.Stop()

	var conn *Conn
	var done bool
	var batch = make([]Message, 0, w.batchSize)
	var resch = make([](chan<- error), 0, w.batchSize)
	var lastFlushAt = time.Now()

	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	for !done {
		var mustFlush bool

		select {
		case wm, ok := <-w.msgs:
			if !ok {
				done, mustFlush = true, true
			} else {
				batch = append(batch, wm.msg)
				resch = append(resch, wm.res)
				mustFlush = len(batch) >= w.batchSize
			}

		case now := <-ticker.C:
			mustFlush = now.Sub(lastFlushAt) > w.batchTimeout
		}

		if mustFlush {
			lastFlushAt = time.Now()

			if len(batch) != 0 {
				var err error
				conn, err = w.write(conn, batch...)

				for _, res := range resch {
					select {
					case res <- err:
					default:
						// The goroutine that generated tht message may have
						// given up if its context was canceled, in that case
						// blocking would result in a dead lock.
					}
				}

				batch = batch[:0]
			}
		}
	}
}

func (w *writer) write(conn *Conn, msgs ...Message) (ret *Conn, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), w.writeTimeout)
	if conn == nil {
		conn, err = w.dial(ctx)
	}

	deadline, _ := ctx.Deadline()
	cancel()

	if conn != nil {
		conn.SetWriteDeadline(deadline)

		_, err = conn.WriteMessages(msgs...)
		conn.Close()
	}

	if err != nil {
		err = &writerError{
			msgs: copyMessages(msgs),
			err:  err,
		}
	}

	ret = conn
	return
}

func (w *writer) dial(ctx context.Context) (conn *Conn, err error) {
	for _, broker := range w.brokers {
		if conn, err = w.dialer.DialLeader(ctx, "tcp", broker, w.topic, w.partition); err == nil {
			break
		}
	}
	return
}

type writerMessage struct {
	msg Message
	res chan<- error
}

type writerError struct {
	msgs []Message
	err  error
}

func (e *writerError) Cause() error {
	return e.err
}

func (e *writerError) Error() string {
	return e.err.Error()
}

func (e *writerError) Temporary() bool {
	return isTemporary(e.err)
}

func (e *writerError) Timeout() bool {
	return isTimeout(e.err)
}

func copyMessages(msgs []Message) []Message {
	cpy := make([]Message, len(msgs))
	copy(cpy, msgs)
	return cpy
}
