package kafka

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// offsetLog is an implementation of the offsetStore interface which uses a
// Kafka partition as backing store for the offsets.
//
// Ideally the partition belongs to a compacted topic, but this is not required
// and the log will work just fine with a regular topic (replays may take a bit
// longer if there are large numbers of offsets recorded in the log).
type offsetLog struct {
	// Addresses of the Kafka brokers tha the log uses to lookup the leader of
	// the topic partition that it uses.
	brokers []string

	// Name of the Kafka topic that the offset log uses to keep track of the
	// offsets.
	topic string

	// Partition tha the offset log uses in the Kafka topic.
	partition int

	// Size limit of the fetch requests to the offset log, default to 8 MB.
	maxBytes int

	// Time limit applied to operations of the offset log.
	readTimeout  time.Duration
	writeTimeout time.Duration

	// Dialer used by the offset log to establish connections to the Kafka
	// brokers. If nil, DefaultDialer is used.
	dialer *Dialer
}

const (
	defaultOffsetLogReadTimeout  = 3 * time.Second
	defaultOffsetLogWriteTimeout = 3 * time.Second
	defaultOffsetLogMaxBytes     = 8 * 1024 * 1024
)

// readOffsets satisfies the offsetStore interface, it returns a list of offsets
// in the partition read by the log.
func (log *offsetLog) readOffsets() ([]offset, error) {
	c, err := log.dialLeader()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	readTimeout := log.readTimeout
	if readTimeout <= 0 {
		readTimeout = defaultOffsetLogReadTimeout
	}
	c.SetReadDeadline(time.Now().Add(readTimeout))

	first, last, err := c.ReadOffsets()
	if err != nil {
		return nil, err
	}

	cursor, err := c.Seek(first, SeekAbsolute)
	if err != nil {
		return nil, err
	}

	maxBytes := log.maxBytes
	if maxBytes <= 0 {
		maxBytes = defaultOffsetLogMaxBytes
	}
	replay := make(map[offsetKey]offsetValue)

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
					replay[key] = value
				} else {
					delete(replay, key)
				}
			}
		}

		if err := b.Close(); err != nil {
			return nil, err
		}

		cursor++
	}

	offsets := make([]offset, 0, len(replay))
	for key, value := range replay {
		offsets = append(offsets, makeOffsetFromKeyAndValue(key, value))
	}
	return offsets, nil
}

// writeOffsets satisfies the offsetStore interface, it writes a set of offsets
// to the Kafka partition managed by the log.
func (log *offsetLog) writeOffsets(offsets ...offset) error {
	return log.write(makeOffsetWriteMessages(offsets))
}

// deleteOffsets satisfies the offsetStore interface, it deletes the offsets from
// the Kafka partition managed by the log.
func (log *offsetLog) deleteOffsets(offsets ...offset) error {
	return log.write(makeOffsetDeleteMessages(offsets))
}

func (log *offsetLog) write(msgs []Message) error {
	c, err := log.dialLeader()
	if err != nil {
		return err
	}
	defer c.Close()
	writeTimeout := log.writeTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultOffsetLogWriteTimeout
	}
	c.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = c.WriteMessages(msgs...)
	return err
}

func (log *offsetLog) dialLeader() (*Conn, error) {
	if len(log.brokers) == 0 {
		return nil, errors.New("no brokers configured on the offset log")
	}
	dialer := log.dialer
	if dialer == nil {
		dialer = DefaultDialer
	}
	broker := log.brokers[rand.Intn(len(log.brokers))]
	return dialer.DialLeader(context.Background(), "tcp", broker, log.topic, log.partition)
}

func makeOffsetWriteMessages(offsets []offset) []Message {
	msgs := make([]Message, len(offsets))
	data := make([]byte, sizeOfOffset*len(offsets))
	pos := 0

	for i := range offsets {
		k, v := offsets[i].toKeyAndValue()

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

func makeOffsetDeleteMessages(offsets []offset) []Message {
	msgs := make([]Message, len(offsets))
	data := make([]byte, sizeOfOffsetKey*len(offsets))
	pos := 0

	for i := range offsets {
		k := offsets[i].toKey()

		key := data[pos : pos+sizeOfOffsetKey]
		pos += sizeOfOffsetKey

		k.writeTo(key)

		msgs[i].Key = key
	}

	return msgs
}
