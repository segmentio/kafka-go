package kafka

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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

// Network satisfies the net.Addr interface, returns "kafka".
func (b Broker) Network() string {
	return "kafka"
}

// String satisfies the net.Addr interface, returns "Host:Port".
func (b Broker) String() string {
	return net.JoinHostPort(b.Host, strconv.Itoa(b.Port))
}

// Partition carries the metadata associated with a kafka partition.
type Partition struct {
	Topic    string
	Leader   Broker
	Replicas []Broker
	Isr      []Broker
	ID       int
}

// Conn represents a connection to a kafka broker.
//
// Instances of Conn are safe to use concurrently from multiple goroutines.
type Conn struct {
	conn net.Conn
	rbuf bufio.Reader
	wbuf bufio.Writer

	clientID      string
	topic         string
	partition     int32
	correlationID int32

	wlock  sync.Mutex
	rlock  sync.Mutex
	mutex  sync.Mutex
	offset int64

	dmutex    sync.RWMutex
	rdeadline time.Time
	wdeadline time.Time
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
	return &Conn{
		conn:      conn,
		rbuf:      *bufio.NewReader(conn),
		wbuf:      *bufio.NewWriter(conn),
		clientID:  config.ClientID,
		topic:     config.Topic,
		partition: int32(config.Partition),
		offset:    -2,
	}
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
	c.rdeadline = t
	c.wdeadline = t
	c.dmutex.Unlock()
	return nil
}

// SetReadDeadline sets the deadline for future Read calls and any
// currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.dmutex.Lock()
	c.rdeadline = t
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
	c.wdeadline = t
	c.dmutex.Unlock()
	return nil
}

// Offset returns the current offset of the connection as pair of integers,
// where the first one is an offset value and the second one indicates how
// to interpret it.
//
// See Seek for more details about the offset and whence values.
func (c *Conn) Offset() (offset int64, whence int) {
	c.mutex.Lock()
	offset = c.offset
	c.mutex.Unlock()
	switch offset {
	case -1:
		offset = 0
		whence = 2
	case -2:
		offset = 0
		whence = 0
	default:
		whence = 1
	}
	return
}

// Seek changes the offset of the connection to offset, interpreted according to
// whence: 0 means relative to the first offset, 1 means relative to the current
// offset, and 2 means relative to the last offset.
// The method returns the new absoluate offset of the connection.
func (c *Conn) Seek(offset int64, whence int) (int64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("invalid negative offset (offset = %d)", offset)
	}
	switch whence {
	case 0, 1, 2:
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

// Read reads the message at the current offset of the connection, advancing
// the offset on success so the next call to Read produces the next message.
// The method returns the number of bytes read, or an error if something went
// wrong.
//
// While it is safe to call read concurrently from multiple goroutines it may
// be hard for the program to prodict the results as the connection offset will
// be read and written by multiple goroutines, they could read duplicates, or
// messages may be seen by only some of the goroutines.
//
// Read satisfies the io.Reader interface.
func (c *Conn) Read(b []byte) (int, error) {
	offset, err := c.Seek(c.Offset())
	if err != nil {
		return 0, err
	}

	var n int
	err = c.readOperation(
		func(deadline time.Time, id int32) error {
			const maxWaitTimeLimit = time.Duration(math.MaxInt32) * time.Millisecond
			// Compute the max wait time with a best-effort attempt to account
			// for network latency.
			maxWaitTime := maxWaitTimeLimit
			if !deadline.IsZero() {
				maxWaitTime = deadline.Sub(time.Now())
				maxWaitTime -= maxWaitTime / 4
			}
			if maxWaitTime > maxWaitTimeLimit {
				maxWaitTime = maxWaitTimeLimit
			}
			return c.writeRequest(fetchRequest, v1, id, fetchRequestV1{
				ReplicaID:   -1,
				MaxWaitTime: int32(maxWaitTime / time.Millisecond),
				MinBytes:    1,
				Topics: []fetchRequestTopicV1{{
					TopicName: c.topic,
					Partitions: []fetchRequestPartitionV1{{
						Partition:   c.partition,
						FetchOffset: offset,
						MaxBytes:    int32(len(b) + 1),
						// We read for the length of the buffer + 1 to detect
						// the case where the message is larger than the output
						// buffer.
					}},
				}},
			})
		},
		func(deadline time.Time, size int) error {
			// Skipping the throttle time since there's no way to report this
			// information with the Read method.
			size, err := discardInt32(&c.rbuf, size)
			if err != nil {
				return err
			}

			// Consume the stream of fetched topic and partition, we skip the
			// topic name because we sent the request for a single topic.
			return expectZeroSize(streamArray(&c.rbuf, size, func(r *bufio.Reader, size int) (int, error) {
				size, err := discardString(&c.rbuf, size)
				if err != nil {
					return size, err
				}
				return streamArray(r, size, func(r *bufio.Reader, size int) (int, error) {
					// Partition header, followed by the message set.
					var p struct {
						Partition           int32
						ErrorCode           int16
						HighwaterMarkOffset int64
						MessageSetSize      int32
					}

					size, err := read(r, size, &p)
					if err != nil {
						return size, err
					}
					if p.ErrorCode != 0 {
						return size, Error(p.ErrorCode)
					}

					done := false

					for size != 0 && err == nil {
						var msgOffset int64
						var msgSize int32

						if msgOffset, msgSize, size, err = readMessageOffsetAndSize(r, size); err != nil {
							continue
						}

						if done {
							// If the fetch returned any trailing messages we
							// just discard them all because we only load one
							// message into the read buffer.
							size, err = discardN(r, size, int(msgSize))
							continue
						}
						done = true

						// Message header, followed by the key and value.
						if _, size, err = readMessageHeader(r, size); err != nil {
							continue
						}
						if _, size, err = readMessageBytes(r, size, b); err != nil {
							continue
						}
						if n, size, err = readMessageBytes(r, size, b); err != nil {
							continue
						}
						offset = msgOffset
					}

					return size, err
				})
			}))
		},
	)

	switch {
	case err != nil:
	case n > len(b):
		n, err = len(b), io.ErrShortBuffer
	default:
		c.mutex.Lock()
		c.offset = offset + 1
		c.mutex.Unlock()
	}

	return n, err
}

// ReadAt reads the message at the given absolute offset, returning the number
// of bytes read and the offset of the next message, or an error if something
// went wrong.
//
// Unlike Read, the method doesn't modify the offset of the connection.
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
	id := c.generateCorrelationID()
	deadline := c.readDeadline()

	c.wlock.Lock()
	c.conn.SetWriteDeadline(deadline)
	err := c.writeRequest(fetchRequest, v1, id, nil) // TODO
	c.wlock.Unlock()

	if err != nil {
		// When an error occurs there's no way to know if the connection is in a
		// recoverable state so we're better off just giving up at this point to
		// avoid any risk of corrupting the following operations.
		c.conn.Close()
		return nil, err
	}

	return &BatchReader{conn: c, id: id}, nil
}

// ReadOffsets returns the absolute first and last offsets of the topic used by
// the connection.
func (c *Conn) ReadOffsets() (first int64, last int64, err error) {
	type result struct {
		off int64
		err error
	}

	resch := make(chan result, 2)
	fetch := func(ptime int64) {
		off := int64(0)
		err := c.writeOperation(
			func(deadline time.Time, id int32) error {
				return c.writeRequest(offsetRequest, v1, id, listOffsetRequestV1{
					ReplicaID: -1,
					Topics: []listOffsetRequestTopicV1{{
						TopicName:  c.topic,
						Partitions: []listOffsetRequestPartitionV1{{Partition: c.partition, Time: ptime}},
					}},
				})
			},
			func(deadline time.Time, size int) error {
				return expectZeroSize(streamArray(&c.rbuf, size, func(r *bufio.Reader, size int) (int, error) {
					// We skip the topic name because we've made a request for
					// a single topic.
					size, err := discardString(r, size)
					if err != nil {
						return size, err
					}

					// Reading the array of partitions, there will be only one
					// partition which gives the offset we're looking for.
					return streamArray(r, size, func(r *bufio.Reader, size int) (int, error) {
						var p partitionOffsetV1
						size, err := read(r, size, &p)
						if err != nil {
							return size, err
						}
						if p.ErrorCode != 0 {
							return size, Error(p.ErrorCode)
						}
						off = p.Offset
						return size, nil
					})
				}))
			},
		)
		resch <- result{off, err}
	}

	// We have to submit two different requests to fetch the first and last
	// offsets because kafka refuses requests that ask for multiple offsets
	// on the same topic and partition.
	go fetch(-1)
	go fetch(-2)

	res1 := <-resch
	res2 := <-resch

	first = minInt64(res1.off, res2.off)
	last = maxInt64(res1.off, res2.off)
	err = coalesceErrors(res1.err, res2.err)
	return
}

// ReadPartitions returns the list of available partitions for the given list of
// topics.
//
// If the method is called with no topic, it uses the topic configured on the
// connection. If there are none, the method fetches all partitions of the kafka
// cluster.
func (c *Conn) ReadPartitions(topics ...string) (partitions []Partition, err error) {
	defaultTopics := [...]string{c.topic}

	if len(topics) == 0 && len(c.topic) != 0 {
		topics = defaultTopics[:]
	}

	err = c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(metadataRequest, v0, id, topicMetadataRequestV0(topics))
		},
		func(deadline time.Time, size int) error {
			var res metadataResponseV0

			if err := c.readResponse(size, &res); err != nil {
				return err
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

			for _, t := range res.Topics {
				if t.TopicErrorCode != 0 {
					continue // TODO: report?
				}
				for _, p := range t.Partitions {
					partitions = append(partitions, Partition{
						Topic:    t.TopicName,
						Leader:   brokers[p.Leader],
						Replicas: makeBrokers(p.Replicas...),
						Isr:      makeBrokers(p.Isr...),
						ID:       int(p.PartitionID),
					})
				}
			}
			return nil
		},
	)
	return
}

// Write writes a message to the kafka broker that this connection was
// established to. The method returns the number of bytes written, or an error
// if something went wrong.
//
// The operation either succeeds or fail, it never partially writes the message.
//
// Write satisfies the io.Writer interface.
func (c *Conn) Write(b []byte) (int, error) {
	msg := makeMessage(timestamp(), nil, b)
	set := messageSet{{
		MessageSize: sizeof(msg),
		Message:     msg,
	}}

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(produceRequest, v2, id, produceRequestV2{
				RequiredAcks: -1,
				Timeout:      10000,
				Topics: []produceRequestTopicV2{{
					TopicName: c.topic,
					Partitions: []produceRequestPartitionV2{{
						Partition:      c.partition,
						MessageSetSize: sizeof(set),
						MessageSet:     set,
					}},
				}},
			})
		},
		func(deadline time.Time, size int) error {
			return expectZeroSize(streamArray(&c.rbuf, size, func(r *bufio.Reader, size int) (int, error) {
				// Skip the topic, we've produced the message to only one topic,
				// no need to waste resources loading it in memory.
				size, err := discardString(r, size)
				if err != nil {
					return size, err
				}

				// Read the list of partitions, there should be only one since
				// we've produced a message to a single partition.
				size, err = streamArray(r, size, func(r *bufio.Reader, size int) (int, error) {
					var p produceResponsePartitionV2
					size, err := read(r, size, &p)
					if err == nil && p.ErrorCode != 0 {
						err = Error(p.ErrorCode)
					}
					return size, err
				})
				if err != nil {
					return size, err
				}

				// The response is trailed by the throttle time, also skipping
				// since it's not interesting here.
				return discardInt32(r, size)
			}))
		},
	)

	if err != nil {
		return 0, err
	}

	return len(b), nil
}

func (c *Conn) writeRequest(apiKey apiKey, apiVersion apiVersion, correlationID int32, req interface{}) error {
	return writeRequest(&c.wbuf, c.requestHeader(apiKey, apiVersion, correlationID), req)
}

func (c *Conn) readResponse(size int, res interface{}) error {
	return readResponse(&c.rbuf, size, res)
}

func (c *Conn) peekResponseSizeAndID() (int32, int32, error) {
	b, err := c.rbuf.Peek(8)
	if err != nil {
		return 0, 0, err
	}
	size, id := makeInt32(b[:4]), makeInt32(b[4:])
	return size, id, nil
}

func (c *Conn) skipResponseSizeAndID() {
	c.rbuf.Discard(8)
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

func (c *Conn) readOperation(write func(time.Time, int32) error, read func(time.Time, int) error) error {
	return c.do(c.readDeadline(), write, read)
}

func (c *Conn) writeOperation(write func(time.Time, int32) error, read func(time.Time, int) error) error {
	return c.do(c.writeDeadline(), write, read)
}

func (c *Conn) do(deadline time.Time, write func(time.Time, int32) error, read func(time.Time, int) error) error {
	id := c.generateCorrelationID()

	c.wlock.Lock()
	c.conn.SetWriteDeadline(deadline)
	err := write(deadline, id)
	c.wlock.Unlock()

	if err != nil {
		// When an error occurs there's no way to know if the connection is in a
		// recoverable state so we're better off just giving up at this point to
		// avoid any risk of corrupting the following operations.
		c.conn.Close()
		return err
	}

	for {
		c.rlock.Lock()
		c.conn.SetReadDeadline(deadline)

		opsize, opid, err := c.peekResponseSizeAndID()
		if err != nil {
			c.conn.Close()
			c.rlock.Unlock()
			return err
		}

		if id == opid {
			c.skipResponseSizeAndID()
			err := read(deadline, int(opsize)-4)
			switch err.(type) {
			case nil, Error:
			default:
				c.conn.Close()
			}
			c.rlock.Unlock()
			return err
		}

		// Optimistically release the read lock if a response has already
		// been received but the current operation is not the target for it.
		c.rlock.Unlock()
		runtime.Gosched()
	}
}

func (c *Conn) requestHeader(apiKey apiKey, apiVersion apiVersion, correlationID int32) requestHeader {
	return requestHeader{
		ApiKey:        int16(apiKey),
		ApiVersion:    int16(apiVersion),
		CorrelationID: correlationID,
		ClientID:      c.clientID,
	}
}

type timeout struct{}

func (*timeout) Error() string   { return "kafka operation timeout" }
func (*timeout) Timeout() bool   { return true }
func (*timeout) Temporary() bool { return false }

func timestamp() int64 {
	return time.Now().UnixNano() / 1e6 // ns => ms
}
