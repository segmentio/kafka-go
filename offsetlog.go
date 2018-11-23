package kafka

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// OffsetLog is an implementation of the OffsetStore interface which uses a
// Kafka partition as backing store for the offsets.
//
// Ideally the partition belongs to a compacted topic, but this is not required
// and the log will work just fine with a regular topic (replays may take a bit
// longer if there are large numbers of offsets recorded in the log).
type OffsetLog struct {
	// Addresses of the Kafka brokers tha the log uses to lookup the leader of
	// the topic partition that it uses.
	Brokers []string

	// Name of the Kafka topic that the offset log uses to keep track of the
	// offsets.
	Topic string

	// Partition tha the offset log uses in the Kafka topic.
	Partition int

	// Size limit of the fetch requests to the offset log, default to 8 MB.
	MaxBytes int

	// Time limit applied to operations of the offset log.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Dialer used by the offset log to establish connections to the Kafka
	// brokers. If nil, DefaultDialer is used.
	Dialer *Dialer
}

const (
	defaultReadTimeout  = 3 * time.Second
	defaultWriteTimeout = 3 * time.Second
	defaultMaxBytes     = 8 * 1024 * 1024
)

// ReadOffsets satisfies the OffsetIter interface, it returns an offset iterator
// which produces all offsets in the partition read by the log.
func (log *OffsetLog) ReadOffsets() OffsetIter {
	c, err := log.dialLeader()
	if err != nil {
		return &errorOffsetIter{err}
	}
	defer c.Close()

	readTimeout := log.readTimeout()
	c.SetReadDeadline(time.Now().Add(readTimeout))

	first, last, err := c.ReadOffsets()
	if err != nil {
		return &errorOffsetIter{err}
	}

	cursor, err := c.Seek(first, SeekAbsolute)
	if err != nil {
		return &errorOffsetIter{err}
	}

	maxBytes := log.maxBytes()
	offsets := make(map[offsetKey]offsetValue)

	for cursor < last {
		c.SetReadDeadline(time.Now().Add(readTimeout))
		b := c.ReadBatch(8, maxBytes)

		for {
			m, err := b.ReadMessage()
			if err != nil {
				break
			}

			if cursor = m.Offset; cursor < last {
				var key offsetKey
				var value offsetValue

				if len(m.Key) != 0 {
					key.readFrom(m.Key)
				}

				if len(m.Value) != 0 {
					value.readFrom(m.Value)
					offsets[key] = value
				} else {
					delete(offsets, key)
				}
			}
		}

		if err := b.Close(); err != nil {
			return &errorOffsetIter{err}
		}

		cursor++
	}

	it := &offsetIter{
		offsets: make([]offset, 0, len(offsets)),
	}

	for key, value := range offsets {
		it.offsets = append(it.offsets, makeOffsetFromKeyAndValue(key, value))
	}

	return it
}

// WriteOffsets satisfies the OffsetIter interface, it writes a set of offsets
// to the Kafka partition managed by the log.
func (log *OffsetLog) WriteOffsets(offsets ...Offset) error {
	return log.write(makeOffsetWriteMessages(offsets))
}

// DeleteOffsets satisfies the OffsetIter interface, it deletes the offsets from
// the Kafka partition managed by the log.
func (log *OffsetLog) DeleteOffsets(offsets ...Offset) error {
	return log.write(makeOffsetDeleteMessages(offsets))
}

func (log *OffsetLog) write(msgs []Message) error {
	c, err := log.dialLeader()
	if err != nil {
		return err
	}
	defer c.Close()
	c.SetWriteDeadline(time.Now().Add(log.writeTimeout()))
	_, err = c.WriteMessages(msgs...)
	return err
}

func (log *OffsetLog) dialer() *Dialer {
	if log.Dialer != nil {
		return log.Dialer
	}
	return DefaultDialer
}

func (log *OffsetLog) dialLeader() (*Conn, error) {
	if len(log.Brokers) == 0 {
		return nil, errors.New("no brokers configured on the offset log")
	}
	broker := log.Brokers[rand.Intn(len(log.Brokers))]
	return log.dialer().DialLeader(context.Background(), "tcp", broker, log.Topic, log.Partition)
}

func (log *OffsetLog) maxBytes() int {
	if log.MaxBytes > 0 {
		return log.MaxBytes
	}
	return defaultMaxBytes
}

func (log *OffsetLog) readTimeout() time.Duration {
	if log.ReadTimeout > 0 {
		return log.ReadTimeout
	}
	return defaultReadTimeout
}

func (log *OffsetLog) writeTimeout() time.Duration {
	if log.ReadTimeout > 0 {
		return log.ReadTimeout
	}
	return defaultWriteTimeout
}

func makeOffsetWriteMessages(offsets []Offset) []Message {
	msgs := make([]Message, len(offsets))
	data := make([]byte, sizeOfOffset*len(offsets))
	pos := 0

	for i := range offsets {
		k, v := makeOffset(offsets[i]).toKeyAndValue()

		key := data[pos : pos+sizeOfOffsetKey]
		pos += sizeOfOffsetKey

		val := data[pos : pos+sizeOfOffsetValue]
		pos += sizeOfOffsetValue

		k.writeTo(key)
		v.writeTo(val)

		msgs[i].Key, msgs[i].Value = key, val
	}

	return msgs
}

func makeOffsetDeleteMessages(offsets []Offset) []Message {
	msgs := make([]Message, len(offsets))
	data := make([]byte, sizeOfOffsetKey*len(offsets))
	pos := 0

	for i := range offsets {
		k := makeOffset(offsets[i]).toKey()

		key := data[pos : pos+sizeOfOffsetKey]
		pos += sizeOfOffsetKey

		k.writeTo(key)

		msgs[i].Key = key
	}

	return msgs
}
