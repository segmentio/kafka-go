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
	Topic    string
	Leader   Broker
	Replicas []Broker
	Isr      []Broker
	ID       int
}

// Message is a data structure representing kafka messages.
type Message struct {
	Offset int64
	Key    []byte
	Value  []byte
	Time   time.Time
}

// Conn represents a connection to a kafka broker.
//
// Instances of Conn are safe to use concurrently from multiple goroutines.
type Conn struct {
	// base network connection
	conn net.Conn

	// offset management (synchronized on the mutex field)
	mutex  sync.Mutex
	offset int64

	// read buffer (synchronized on rlock)
	rlock sync.Mutex
	rbuf  bufio.Reader

	// write buffer (synchronized on wlock)
	wlock sync.Mutex
	wbuf  bufio.Writer

	// deadline management
	wdeadline connDeadline
	rdeadline connDeadline

	// immutable values of the connection object
	clientID      string
	topic         string
	partition     int32
	correlationID int32
	fetchMinSize  int32
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

	return &Conn{
		conn:      conn,
		rbuf:      *bufio.NewReader(conn),
		wbuf:      *bufio.NewWriter(conn),
		clientID:  config.ClientID,
		topic:     config.Topic,
		partition: int32(config.Partition),
		offset:    -2,

		// The fetch request needs to ask for a MaxBytes value that is at least
		// enough to load the control data of the response. To avoid having to
		// recompute it on every read, it is cached here in the Conn value.
		fetchMinSize: sizeof(fetchResponseV1{
			Topics: []fetchResponseTopicV1{{
				TopicName: config.Topic,
				Partitions: []fetchResponsePartitionV1{{
					Partition: int32(config.Partition),
				}},
			}},
		}),
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
	c.rdeadline.setDeadline(t)
	c.wdeadline.setDeadline(t)
	return nil
}

// SetReadDeadline sets the deadline for future Read calls and any
// currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.rdeadline.setDeadline(t)
	return nil
}

// SetWriteDeadline sets the deadline for future Write calls and any
// currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that some of the
// data was successfully written.
// A zero value for t means Write will not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.wdeadline.setDeadline(t)
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
	n, offset, err := c.ReadAt(b, offset)
	if err == nil {
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
	var adjustedDeadline time.Time
	var n int
	var err = c.readOperation(
		func(deadline time.Time, id int32) error {
			now := time.Now()
			adjustedDeadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
			return c.writeRequest(fetchRequest, v1, id, fetchRequestV1{
				ReplicaID:   -1,
				MinBytes:    1,
				MaxWaitTime: milliseconds(deadlineToTimeout(adjustedDeadline, now)),
				Topics: []fetchRequestTopicV1{{
					TopicName: c.topic,
					Partitions: []fetchRequestPartitionV1{{
						Partition:   c.partition,
						MaxBytes:    c.fetchMinSize + int32(len(b)),
						FetchOffset: offset,
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

				// As an "optimization" kafka truncates the returned response
				// after producing MaxBytes, which could then cause the code to
				// return errShortRead.
				// Because we read at least c.fetchMinSize bytes we should be
				// able to decode all the control values for the first message,
				// and errShortRead should only happen when reading the message
				// key or value.
				// I'm not sure this is rock solid and there may be some weird
				// edge cases...
				// There's just so much we can do with questionable design.
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
						return ignoreShortRead(size, err)
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
						if _, _, size, err = readMessageHeader(r, size); err != nil {
							continue
						}
						if n, size, err = readMessageBytes(r, size, b); err != nil {
							continue
						}
						if n, size, err = readMessageBytes(r, size, b); err != nil {
							continue
						}
						offset = msgOffset
					}

					return ignoreShortRead(size, err)
				})
			}))
		},
	)

	if err == nil {
		if n > len(b) {
			n, err = len(b), io.ErrShortBuffer
		} else if n == 0 && time.Now().After(adjustedDeadline) {
			// Because we use the adjusted deadline we could end up returning
			// before the actual deadline occurred. This is necessary otherwise
			// timing out the connection for read could end up leaving it in an
			// unpredictable state, which would require closing it.
			// This design decision was main to maximize the changes of keeping
			// the connection open, the trade off being to lose precision on the
			// read deadline management.
			err = RequestTimedOut
		}
	}

	return n, offset, err
}

func (c *Conn) ReadBatch(maxBytes int) *Batch {
	return nil
}

// ReadOffset returns the offset of the first message with a timestamp equal or
// greater to t.
func (c *Conn) ReadOffset(t time.Time) (int64, error) {
	return c.readOffset(timeToTimestamp(t))
}

// ReadOffsets returns the absolute first and last offsets of the topic used by
// the connection.
func (c *Conn) ReadOffsets() (first int64, last int64, err error) {
	// We have to submit two different requests to fetch the first and last
	// offsets because kafka refuses requests that ask for multiple offsets
	// on the same topic and partition.
	if last, err = c.readOffset(-1); err != nil {
		return
	}
	first, err = c.readOffset(-2)
	return
}

func (c *Conn) readOffset(t int64) (offset int64, err error) {
	err = c.readOperation(
		func(deadline time.Time, id int32) error {
			return c.writeRequest(offsetRequest, v1, id, listOffsetRequestV1{
				ReplicaID: -1,
				Topics: []listOffsetRequestTopicV1{{
					TopicName:  c.topic,
					Partitions: []listOffsetRequestPartitionV1{{Partition: c.partition, Time: t}},
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
					offset = p.Offset
					return size, nil
				})
			}))
		},
	)
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
				if t.TopicErrorCode != 0 && t.TopicName == c.topic {
					// We only report errors if they happened for the topic of
					// the connection, otherwise the topic will simply have no
					// partitions in the result set.
					return Error(t.TopicErrorCode)
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
	return c.WriteMessages(Message{Value: b})
}

// WriteMessages writes a batch of messages to the connection's topic and
// partition, returning the number of bytes written. The write is an atomic
// operation, it either fully succeeds or fails.
func (c *Conn) WriteMessages(msgs ...Message) (int, error) {
	if len(msgs) == 0 {
		return 0, nil
	}

	var n int
	var set = make(messageSet, len(msgs))

	for i, msg := range msgs {
		n += len(msg.Key) + len(msg.Value)
		m := message{
			MagicByte: 1,
			Key:       msg.Key,
			Value:     msg.Value,
			Timestamp: timeToTimestamp(msg.Time),
		}
		m.CRC = m.crc32()
		set[i] = messageSetItem{
			Offset:      msg.Offset,
			MessageSize: sizeof(m),
			Message:     m,
		}
	}

	err := c.writeOperation(
		func(deadline time.Time, id int32) error {
			now := time.Now()
			deadline = adjustDeadlineForRTT(deadline, now, defaultRTT)
			return c.writeRequest(produceRequest, v2, id, produceRequestV2{
				RequiredAcks: -1,
				Timeout:      milliseconds(deadlineToTimeout(deadline, now)),
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
		n = 0
	}

	return n, err
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
	return c.rdeadline.deadline()
}

func (c *Conn) writeDeadline() time.Time {
	return c.wdeadline.deadline()
}

func (c *Conn) readOperation(write writeFunc, read readFunc) error {
	return c.do(&c.rdeadline, write, read)
}

func (c *Conn) writeOperation(write writeFunc, read readFunc) error {
	return c.do(&c.wdeadline, write, read)
}

func (c *Conn) do(d *connDeadline, write writeFunc, read readFunc) error {
	id, err := c.doRequest(d, write)
	if err != nil {
		return err
	}

	deadline, size, lock, err := c.waitResponse(d, id)
	if err != nil {
		return err
	}

	if err = read(deadline, int(size)-4); err != nil {
		switch err.(type) {
		case Error:
		default:
			c.conn.Close()
		}
	}

	d.unsetConnReadDeadline()
	lock.Unlock()
	return err
}

func (c *Conn) doRequest(d *connDeadline, write writeFunc) (id int32, err error) {
	id = c.generateCorrelationID()

	c.wlock.Lock()
	err = write(d.setConnWriteDeadline(c.conn), id)
	d.unsetConnWriteDeadline()
	c.wlock.Unlock()

	if err != nil {
		// When an error occurs there's no way to know if the connection is in a
		// recoverable state so we're better off just giving up at this point to
		// avoid any risk of corrupting the following operations.
		c.conn.Close()
	}

	return
}

func (c *Conn) waitResponse(d *connDeadline, id int32) (deadline time.Time, size int32, lock *sync.Mutex, err error) {
	for {
		var rsz int32
		var rid int32

		c.rlock.Lock()
		deadline = d.setConnReadDeadline(c.conn)

		if rsz, rid, err = c.peekResponseSizeAndID(); err != nil {
			d.unsetConnReadDeadline()
			c.conn.Close()
			c.rlock.Unlock()
			return
		}

		if id == rid {
			c.skipResponseSizeAndID()
			size, lock = rsz, &c.rlock
			return
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

// connDeadline is a helper type to implement read/write deadline management on
// the kafka connection.
type connDeadline struct {
	mutex sync.Mutex
	value time.Time
	rconn net.Conn
	wconn net.Conn
}

func (d *connDeadline) deadline() time.Time {
	d.mutex.Lock()
	t := d.value
	d.mutex.Unlock()
	return t
}

func (d *connDeadline) setDeadline(t time.Time) {
	d.mutex.Lock()
	d.value = t

	if d.rconn != nil {
		d.rconn.SetReadDeadline(t)
	}

	if d.wconn != nil {
		d.wconn.SetWriteDeadline(t)
	}

	d.mutex.Unlock()
}

func (d *connDeadline) setConnReadDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.rconn = conn
	d.rconn.SetReadDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) setConnWriteDeadline(conn net.Conn) time.Time {
	d.mutex.Lock()
	deadline := d.value
	d.wconn = conn
	d.wconn.SetWriteDeadline(deadline)
	d.mutex.Unlock()
	return deadline
}

func (d *connDeadline) unsetConnReadDeadline() {
	d.mutex.Lock()
	d.rconn = nil
	d.mutex.Unlock()
}

func (d *connDeadline) unsetConnWriteDeadline() {
	d.mutex.Lock()
	d.wconn = nil
	d.mutex.Unlock()
}

// writeFunc is the type of functions passed to (*Conn).do to write a request
// to the kafka connection.
type writeFunc func(deadline time.Time, id int32) error

// readFunc is the type of functions passed to (*Conn).do to read a response
// from the kafka connection.
type readFunc func(deadline time.Time, size int) error
