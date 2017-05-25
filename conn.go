package kafka

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Broker carries the metadata associated with a kafka broker.
type Broker struct {
	Host string
	Port int
	ID   int
}

// Partition carries the metadata associated with a kafka partition.
type Partition struct {
	Leader   Broker
	Replicas []Broker
	Isr      []Broker
	ID       int
}

// Conn represents a connection to a kafka broker.
//
// Instances of Conn are safe to use concurrently from multiple goroutines.
type Conn struct {
	conn  net.Conn
	crbuf bufio.Reader
	cwbuf bufio.Writer

	clientID      string
	topic         string
	partition     int32
	correlationID int32

	mutex  sync.Mutex
	offset int64

	dmutex    sync.RWMutex
	rconn     net.Conn
	wconn     net.Conn
	rdeadline time.Time
	wdeadline time.Time

	oplock     sync.Mutex
	opmutex    sync.Mutex
	operations map[int32]*connOp
}

// ConnConfig is a configuration object used to create new instances of Conn.
type ConnConfig struct {
	ClientID  string
	Topic     string
	Partition int
}

var (
	// DefaultClientID is the default value used as ClientID of kafka
	// connections.
	DefaultClientID string
)

func init() {
	progname := filepath.Base(os.Args[0])
	hostname, _ := os.Hostname()
	DefaultClientID = fmt.Sprintf("%s@%s (github.com/segmentio/kafka-go)", progname, hostname)
}

// NewConn returns a new kafka connection for the given topic and partition.
func NewConn(conn net.Conn, topic string, partition int) *Conn {
	return NewConnWith(conn, ConnConfig{
		Topic:     topic,
		Partition: partition,
	})
}

// NewConnWith returns a new kafka connection configured with config.
func NewConnWith(conn net.Conn, config ConnConfig) *Conn {
	if len(config.ClientID) == 0 {
		config.ClientID = DefaultClientID
	}

	if config.Partition < 0 || config.Partition > math.MaxInt32 {
		panic(fmt.Sprintf("invalid partition number: %d", config.Partition))
	}

	conn.SetDeadline(time.Time{})

	c := &Conn{
		conn:       conn,
		clientID:   config.ClientID,
		topic:      config.Topic,
		partition:  int32(config.Partition),
		offset:     -2,
		operations: make(map[int32]*connOp),
	}
	c.crbuf.Reset(conn)
	c.cwbuf.Reset(conn)

	go c.readLoop()
	return c
}

// Close closes the kafka connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated with the connection.
// It is equivalent to calling both SetReadDeadline and SetWriteDeadline.
//
// A deadline is an absolute time after which I/O operations fail with a timeout
// (see type Error) instead of blocking. The deadline applies to all future and
// pending I/O, not just the immediately following call to Read or Write. After
// a deadline has been exceeded, the connection may be closed if it was found to
// be in an unrecoverable state.
//
// A zero value for t means I/O operations will not time out.
func (c *Conn) SetDeadline(t time.Time) error {
	c.dmutex.Lock()
	c.setDeadline(t, t)
	c.dmutex.Unlock()
	return nil
}

// SetReadDeadline sets the deadline for future Read calls and any
// currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.dmutex.Lock()
	c.setDeadline(t, c.wdeadline)
	c.dmutex.Lock()
	return nil
}

// SetWriteDeadline sets the deadline for future Write calls and any
// currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that some of the
// data was successfully written.
// A zero value for t means Write will not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.dmutex.Lock()
	c.setDeadline(c.rdeadline, t)
	c.dmutex.Unlock()
	return nil
}

func (c *Conn) setDeadline(rdeadline time.Time, wdeadline time.Time) {
	if c.rconn != nil {
		c.rconn.SetReadDeadline(rdeadline)
	}
	if c.wconn != nil {
		c.wconn.SetWriteDeadline(wdeadline)
	}
	c.rdeadline = rdeadline
	c.wdeadline = wdeadline

	now := time.Now()
	c.opmutex.Lock()
	for _, op := range c.operations {
		var deadline time.Time
		switch op.typ {
		case readOp:
			deadline = rdeadline
		case writeOp:
			deadline = wdeadline
		}
		switch {
		case deadline.IsZero():
			op.timer.Stop()
		case deadline.After(op.deadline):
			op.timer.Reset(deadline.Sub(now))
		default:
			op.cancel(&timeout{})
		}
		op.deadline = deadline
	}
	c.opmutex.Unlock()
}

func (c *Conn) Offset() (offset int64, whence int) {
	c.mutex.Lock()
	offset = c.offset
	c.mutex.Unlock()
	switch offset {
	case -1:
		whence = 2
	case -2:
		whence = 0
	default:
		whence = 1
	}
	return
}

func (c *Conn) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("invalid negative offset (offset = %d)", offset)
	}
	switch whence {
	case 0, 1, 2:
		return 0, nil
	default:
		return 0, fmt.Errorf("the whence value has to be 0, 1, or 2 (whence = %d)", whence)
	}

	if whence == 1 {
		c.mutex.Lock()
		unchanged := offset == c.offset
		c.mutex.Unlock()
		if unchanged {
			return offset, nil
		}
	}

	first, last, err := c.ReadOffsets()
	if err != nil {
		return 0, err
	}

	switch whence {
	case 0:
		offset = first + offset
	case 2:
		offset = last - offset
	}

	if offset < first || offset > last {
		return 0, OffsetOutOfRange
	}

	c.mutex.Lock()
	c.offset = offset
	c.mutex.Unlock()
	return offset, nil
}

func (c *Conn) Read(b []byte) (int, error) {
	batch, err := c.ReadBatch(1)
	if err != nil {
		return 0, err
	}
	n, err := batch.Read(b)
	if err != nil {
		batch.Close()
		return 0, err
	}
	return n, batch.Close()
}

func (c *Conn) ReadAt(b []byte, offset int64) (int, int64, error) {
	batch, err := c.ReadBatchAt(1, offset)
	if err != nil {
		return 0, 0, err
	}
	n, err := batch.Read(b)
	if err != nil {
		batch.Close()
		return 0, 0, err
	}
	return n, batch.Offset(), batch.Close()
}

func (c *Conn) ReadBatch(size int) (*BatchReader, error) {
	offset, err := c.Seek(c.Offset())
	if err != nil {
		return nil, err
	}
	batch, err := c.ReadBatchAt(size, offset)
	if err != nil {
		return nil, err
	}
	batch.update = true // we want the batch to update the connection's offset
	return batch, nil
}

func (c *Conn) ReadBatchAt(size int, offset int64) (*BatchReader, error) {
	// There's a bit of code duplication here with the (*Conn).do method, this
	// is done to support the streaming API of Batch and not have to load the
	// fetch response all in memory.
	op := newConnOp(readOp, c.readDeadline())
	id := c.generateCorrelationID()
	c.addOperation(id, op)

	c.oplock.Lock()
	c.setConnWriteDeadline(op.deadline)
	err := c.writeRequest(fetchRequest, v1, id, nil) // TODO
	c.unsetConnWriteDeadline()
	c.oplock.Unlock()

	if err == nil {
		select {
		case <-op.ready:
			c.setConnReadDeadline(op.deadline)
		case <-op.timer.C:
			err = &timeout{}
		case <-op.done:
			err = op.error()
		}
	}

	if err != nil {
		// When an error occurs there's no way to know if the connection is in a
		// recoverable state so we're better off just giving up at this point to
		// avoid any risk of corrupting the following operations.
		c.conn.Close()
		c.removeOperation(id)
		op.cancel(err)
		return nil, err
	}

	return &BatchReader{conn: c, op: op, id: id}, nil
}

func (c *Conn) ReadOffsets() (first int64, last int64, err error) {
	first, last = -1, -1

	err = c.writeOperation(
		func(id int32) error {
			return c.writeRequest(offsetRequest, v1, id, listOffsetRequest{
				Topics: []listOffsetRequestTopic{{
					TopicName: c.topic,
					Partitions: []listOffsetRequestPartition{
						{Partition: c.partition, Time: -2},
						{Partition: c.partition, Time: -1},
					},
				}},
			})
		},
		func(size int64) error {
			var res []listOffsetResponse
			if err := c.readResponse(size, &res); err != nil {
				return err
			}
			for _, r := range res {
				if r.TopicName != c.topic {
					continue
				}
				for _, p := range r.PartitionOffsets {
					if p.Partition != c.partition {
						continue
					}
					if p.ErrorCode != 0 {
						return Error(p.ErrorCode)
					}
					for _, offset := range p.Offsets {
						if first < 0 || offset < first {
							first = offset
						}
						if last < 0 || offset > last {
							last = offset
						}
					}
				}
			}
			return nil
		},
	)

	if err == nil && (first < 0 || last < 0) {
		err = UnknownTopicOrPartition
	}

	return
}

func (c *Conn) ReadPartitions() (partitions []Partition, err error) {
	err = c.readOperation(
		func(id int32) error {
			return c.writeRequest(metadataRequest, v0, id, topicMetadataRequest{c.topic})
		},
		func(size int64) error {
			var res metadataResponse
			if err := c.readResponse(size, &res); err != nil {
				return err
			}

			switch len(res.Topics) {
			case 1:
			case 0:
				return fmt.Errorf("no topic metadata returned by the kafka server for %q", c.topic)
			default:
				return fmt.Errorf("too many topic metadata returned by the kafka server for %q", c.topic)
			}

			if res.Topics[0].TopicErrorCode != 0 {
				return Error(res.Topics[0].TopicErrorCode)
			}

			brokers := make(map[int32]Broker, len(res.Brokers))
			for _, b := range res.Brokers {
				brokers[b.NodeID] = Broker{
					Host: b.Host,
					Port: int(b.Port),
					ID:   int(b.NodeID),
				}
			}

			makeBrokers := func(ids ...int32) []Broker {
				b := make([]Broker, len(ids))
				for i, id := range ids {
					b[i] = brokers[id]
				}
				return b
			}

			for _, p := range res.Topics[0].Partitions {
				partitions = append(partitions, Partition{
					Leader:   brokers[p.Leader],
					Replicas: makeBrokers(p.Replicas...),
					Isr:      makeBrokers(p.Isr...),
					ID:       int(p.PartitionID),
				})
			}
			return nil
		},
	)
	return
}

func (c *Conn) Write(b []byte) (int, error) {
	msg := makeMessage(timestamp(), nil, b)
	set := messageSet{{
		MessageSize: sizeof(msg),
		Message:     msg,
	}}

	err := c.writeOperation(
		func(id int32) error {
			return c.writeRequest(produceRequest, v2, id, produceRequestType{
				RequiredAcks: -1,
				Timeout:      10000,
				Topics: []produceRequestTopic{{
					TopicName: c.topic,
					Partitions: []produceRequestPartition{{
						Partition:      c.partition,
						MessageSetSize: sizeof(set),
						MessageSet:     set,
					}},
				}},
			})
		},
		func(size int64) error {
			var res produceResponse
			if err := c.readResponse(size, &res); err != nil {
				return err
			}
			for _, t := range res.Topics {
				if t.TopicName != c.topic {
					continue
				}
				for _, p := range t.Partitions {
					if p.Partition != c.partition {
						continue
					}
					if p.ErrorCode != 0 {
						return Error(p.ErrorCode)
					}
				}
			}
			return nil
		},
	)

	if err != nil {
		return 0, err
	}

	return len(b), nil
}

func (c *Conn) writeRequest(apiKey apiKey, apiVersion apiVersion, correlationID int32, req interface{}) error {
	return writeRequest(&c.cwbuf, c.requestHeader(apiKey, apiVersion, correlationID), req)
}

func (c *Conn) readResponse(size int64, res interface{}) error {
	return readResponse(&c.crbuf, size, &res)
}

func (c *Conn) readLoop() {
	for {
		c.conn.SetReadDeadline(time.Time{})
		b, err := c.crbuf.Peek(8)
		if err != nil {
			break
		}

		size := int64(makeInt32(b[:4]))
		id := makeInt32(b[4:])
		c.crbuf.Discard(8)

		c.opmutex.Lock()
		op, ok := c.operations[id]
		c.opmutex.Unlock()
		if ok {
			select {
			case <-op.done:
			case op.ready <- size - 8:
				<-op.done // wait for the program to consume the response
				continue
			}
		}
		discard(&c.crbuf, c.conn, size)
	}

	c.conn.Close()
	c.opmutex.Lock()
	for _, op := range c.operations {
		op.cancel(io.ErrClosedPipe)
	}
	c.opmutex.Unlock()
}

func (c *Conn) addOperation(id int32, op *connOp) {
	c.opmutex.Lock()
	c.operations[id] = op
	c.opmutex.Unlock()
}

func (c *Conn) removeOperation(id int32) {
	c.opmutex.Lock()
	delete(c.operations, id)
	c.opmutex.Unlock()
}

func (c *Conn) generateCorrelationID() int32 {
	return atomic.AddInt32(&c.correlationID, 1)
}

func (c *Conn) readDeadline() time.Time {
	c.dmutex.RLock()
	t := c.rdeadline
	c.dmutex.RUnlock()
	return t
}

func (c *Conn) writeDeadline() time.Time {
	c.dmutex.RLock()
	t := c.wdeadline
	c.dmutex.RUnlock()
	return t
}

func (c *Conn) readOperation(write func(int32) error, read func(int64) error) error {
	return c.do(newConnOp(readOp, c.readDeadline()), write, read)
}

func (c *Conn) writeOperation(write func(int32) error, read func(int64) error) error {
	return c.do(newConnOp(writeOp, c.writeDeadline()), write, read)
}

func (c *Conn) do(op *connOp, write func(int32) error, read func(int64) error) (err error) {
	// TODO: there's a race if SetDeadline, SetReadDeadline, or SetWriteDeadline
	// is called after the operation was created but before it is inserted into
	// the map of active operations. In that case the deadline used by that
	// operation could be wrong... Not sure if it's worth adding more contention
	// to attempt to solve this edge case, most applications won't be making
	// changes to the deadlines concurrently while reading or writing.
	id := c.generateCorrelationID()
	c.addOperation(id, op)

	// This lock synchronizes writes to the connection, it ensures that only one
	// goroutines will being sending requests at a time. At first sight it may
	// look like it would not respect the current operation's timer expiration
	// but actually all in-flight operation timers are adjusted based on the
	// same deadline.
	c.oplock.Lock()
	c.setConnWriteDeadline(op.deadline)
	err = write(id)
	c.unsetConnWriteDeadline()
	c.oplock.Unlock()

	if err == nil {
		select {
		case size := <-op.ready:
			c.setConnReadDeadline(op.deadline)
			err = read(size)
			c.unsetConnReadDeadline()
		case <-op.timer.C:
			err = &timeout{}
		case <-op.done:
			err = op.error()
		}
	}

	if err != nil {
		// When an error occurs there's no way to know if the connection is in a
		// recoverable state so we're better off just giving up at this point to
		// avoid any risk of corrupting the following operations.
		c.conn.Close()
	}

	c.removeOperation(id)
	op.cancel(err)
	return
}

func (c *Conn) setConnReadDeadline(t time.Time) {
	c.dmutex.Lock()
	c.conn.SetReadDeadline(t)
	c.rconn = c.conn
	c.dmutex.Unlock()
}

func (c *Conn) unsetConnReadDeadline() {
	c.dmutex.Lock()
	c.rconn = nil
	c.dmutex.Unlock()
}

func (c *Conn) setConnWriteDeadline(t time.Time) {
	c.dmutex.Lock()
	c.conn.SetWriteDeadline(t)
	c.wconn = c.conn
	c.dmutex.Unlock()
}

func (c *Conn) unsetConnWriteDeadline() {
	c.dmutex.Lock()
	c.wconn = nil
	c.dmutex.Unlock()
}

func (c *Conn) requestHeader(apiKey apiKey, apiVersion apiVersion, correlationID int32) requestHeader {
	return requestHeader{
		ApiKey:        int16(apiKey),
		ApiVersion:    int16(apiVersion),
		CorrelationID: correlationID,
		ClientID:      c.clientID,
	}
}

func discard(r io.Reader, c io.Closer, n int64) {
	if _, err := io.CopyN(ioutil.Discard, r, n); err != nil {
		c.Close()
	}
}

// connOp is a context-like type that carries the synchronization primitives and
// the state of an active operation.
type connOp struct {
	typ      connOpType
	ready    chan int64
	done     chan struct{}
	timer    *time.Timer
	deadline time.Time
	once     sync.Once
	err      atomic.Value
}

func newConnOp(typ connOpType, deadline time.Time) *connOp {
	var timer *time.Timer
	if deadline.IsZero() {
		timer = time.NewTimer(time.Hour)
		timer.Stop()
	} else {
		timer = time.NewTimer(deadline.Sub(time.Now()))
	}
	return &connOp{
		typ:      typ,
		ready:    make(chan int64),
		done:     make(chan struct{}),
		timer:    timer,
		deadline: deadline,
	}
}

func (op *connOp) error() error {
	err, _ := op.err.Load().(error)
	return err
}

func (op *connOp) cancel(err error) {
	op.once.Do(func() {
		op.err.Store(err)
		op.timer.Stop()
		close(op.done)
	})
}

type connOpType int

const (
	readOp connOpType = iota
	writeOp
)

type timeout struct{}

func (*timeout) Error() string   { return "kafka operation timeout" }
func (*timeout) Timeout() bool   { return true }
func (*timeout) Temporary() bool { return false }

func timestamp() int64 {
	return time.Now().UnixNano() / 1e6 // ns => ms
}
