package kafka

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

type writable interface {
	writeTo(*bufio.Writer)
}

func writeInt8(w *bufio.Writer, i int8) {
	w.WriteByte(byte(i))
}

func writeInt16(w *bufio.Writer, i int16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(i))
	w.WriteByte(b[0])
	w.WriteByte(b[1])
}

func writeInt32(w *bufio.Writer, i int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(i))
	w.WriteByte(b[0])
	w.WriteByte(b[1])
	w.WriteByte(b[2])
	w.WriteByte(b[3])
}

func writeInt64(w *bufio.Writer, i int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	w.WriteByte(b[0])
	w.WriteByte(b[1])
	w.WriteByte(b[2])
	w.WriteByte(b[3])
	w.WriteByte(b[4])
	w.WriteByte(b[5])
	w.WriteByte(b[6])
	w.WriteByte(b[7])
}

func writeVarInt(w *bufio.Writer, i int64) {
	i = i<<1 ^ i>>63
	for i&0x7f != i {
		w.WriteByte(byte(i&0x7f | 0x80))
		i >>= 7
	}
	w.WriteByte(byte(i))
}

func varIntLen(i int64) (l int) {
	i = i<<1 ^ i>>63
	for i&0x7f != i {
		l++
		i >>= 7
	}
	l++
	return l
}

func writeString(w *bufio.Writer, s string) {
	writeInt16(w, int16(len(s)))
	w.WriteString(s)
}

func writeBytes(w *bufio.Writer, b []byte) {
	n := len(b)
	if b == nil {
		n = -1
	}
	writeInt32(w, int32(n))
	w.Write(b)
}

func writeBool(w *bufio.Writer, b bool) {
	v := int8(0)
	if b {
		v = 1
	}
	writeInt8(w, v)
}

func writeArrayLen(w *bufio.Writer, n int) {
	writeInt32(w, int32(n))
}

func writeArray(w *bufio.Writer, n int, f func(int)) {
	writeArrayLen(w, n)
	for i := 0; i != n; i++ {
		f(i)
	}
}

func writeStringArray(w *bufio.Writer, a []string) {
	writeArray(w, len(a), func(i int) { writeString(w, a[i]) })
}

func writeInt32Array(w *bufio.Writer, a []int32) {
	writeArray(w, len(a), func(i int) { writeInt32(w, a[i]) })
}

func write(w *bufio.Writer, a interface{}) {
	switch v := a.(type) {
	case int8:
		writeInt8(w, v)
	case int16:
		writeInt16(w, v)
	case int32:
		writeInt32(w, v)
	case int64:
		writeInt64(w, v)
	case string:
		writeString(w, v)
	case []byte:
		writeBytes(w, v)
	case bool:
		writeBool(w, v)
	case writable:
		v.writeTo(w)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

// The functions bellow are used as optimizations to avoid dynamic memory
// allocations that occur when building the data structures representing the
// kafka protocol requests.

func writeFetchRequestV2(w *bufio.Writer, correlationID int32, clientID, topic string, partition int32, offset int64, minBytes, maxBytes int, maxWait time.Duration) error {
	h := requestHeader{
		ApiKey:        int16(fetchRequest),
		ApiVersion:    int16(v2),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // max wait time
		4 + // min bytes
		4 + // topic array length
		sizeofString(topic) +
		4 + // partition array length
		4 + // partition
		8 + // offset
		4 // max bytes

	h.writeTo(w)
	writeInt32(w, -1) // replica ID
	writeInt32(w, milliseconds(maxWait))
	writeInt32(w, int32(minBytes))

	// topic array
	writeArrayLen(w, 1)
	writeString(w, topic)

	// partition array
	writeArrayLen(w, 1)
	writeInt32(w, partition)
	writeInt64(w, offset)
	writeInt32(w, int32(maxBytes))

	return w.Flush()
}

func writeListOffsetRequestV1(w *bufio.Writer, correlationID int32, clientID, topic string, partition int32, time int64) error {
	h := requestHeader{
		ApiKey:        int16(listOffsetRequest),
		ApiVersion:    int16(v1),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		8 // time

	h.writeTo(w)
	writeInt32(w, -1) // replica ID

	// topic array
	writeArrayLen(w, 1)
	writeString(w, topic)

	// partition array
	writeArrayLen(w, 1)
	writeInt32(w, partition)
	writeInt64(w, time)

	return w.Flush()
}

func hasHeaders(msgs ...Message) bool {
	for _, msg := range msgs {
		if len(msg.Headers) > 0 {
			return true
		}
	}
	return false
}

func writeProduceRequestV2(w *bufio.Writer, codec CompressionCodec, correlationID int32, clientID, topic string, partition int32, timeout time.Duration, requiredAcks int16, msgs ...Message) (err error) {
	var msgBuf []byte
	if hasHeaders(msgs...) {
		msgBuf, err = writeRecordBatch(codec, correlationID, clientID, topic, partition, timeout, requiredAcks, msgs...)
	} else {
		msgBuf, err = writeMessageSet(codec, correlationID, clientID, topic, partition, timeout, requiredAcks, msgs...)
	}
	if err != nil {
		return
	}

	var size int32 = int32(len(msgBuf))

	h := requestHeader{
		ApiKey:        int16(produceRequest),
		ApiVersion:    int16(v2),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		2 + // required acks
		4 + // timeout
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		4 + // message set size
		size

	h.writeTo(w)
	writeInt16(w, requiredAcks) // required acks
	writeInt32(w, milliseconds(timeout))

	// topic array
	writeArrayLen(w, 1)
	writeString(w, topic)

	// partition array
	writeArrayLen(w, 1)
	writeInt32(w, partition)
	writeInt32(w, size)
	w.Write(msgBuf)
	return w.Flush()
}

func writeMessageSet(codec CompressionCodec, correlationID int32, clientId, topic string, partition int32, timeout time.Duration, requiredAcks int16, msgs ...Message) ([]byte, error) {
	var size int32
	attributes := int8(CompressionNoneCode)

	// if compressing, replace the slice of messages with a single compressed
	// message set.
	if codec != nil {
		var err error
		if msgs, err = compress(codec, msgs...); err != nil {
			return nil, err
		}
		attributes = codec.Code()
	}

	for _, msg := range msgs {
		size += 8 + // offset
			4 + // message size
			4 + // crc
			1 + // magic byte
			1 + // attributes
			8 + // timestamp
			sizeofBytes(msg.Key) +
			sizeofBytes(msg.Value)
	}

	buf := &bytes.Buffer{}
	buf.Grow(int(size))
	bufWriter := bufio.NewWriter(buf)

	for _, msg := range msgs {
		writeMessage(bufWriter, msg.Offset, attributes, msg.Time, msg.Key, msg.Value)
	}
	bufWriter.Flush()

	return buf.Bytes(), nil
}

func writeRecordBatch(codec CompressionCodec, correlationID int32, clientId, topic string, partition int32, timeout time.Duration, requiredAcks int16, msgs ...Message) ([]byte, error) {
	var size int32
	size = 8 + // base offset
		4 + // batch length
		4 + // partition leader epoch
		1 + // magic
		4 + // crc
		2 + // attributes
		4 + // last offset delta
		8 + // first timestamp
		8 + // max timestamp
		8 + // producer id
		2 + // producer epoch
		4 // base sequence

	for _, msg := range msgs {
		size += estimatedRecordSize(&msg)
	}

	buf := &bytes.Buffer{}
	buf.Grow(int(size))
	bufWriter := bufio.NewWriter(buf)

	baseTime := msgs[0].Time

	baseOffset := baseOffset(msgs...)

	writeInt64(bufWriter, baseOffset)

	remainderBuf := &bytes.Buffer{}
	remainderBuf.Grow(int(size - 12)) // 12 = batch length + base offset sizes
	remainderWriter := bufio.NewWriter(remainderBuf)

	writeInt32(remainderWriter, -1) // partition leader epoch
	writeInt8(remainderWriter, 2)   // magic byte

	crcBuf := &bytes.Buffer{}
	crcBuf.Grow(int(size - 12)) // 12 = batch length + base offset sizes
	crcWriter := bufio.NewWriter(crcBuf)

	writeInt16(crcWriter, 0) // attributes no compression, timestamp type 0 - create time, not part of a transaction, no control messages
	writeInt32(crcWriter, int32(msgs[len(msgs)-1].Offset-baseOffset))
	writeInt64(crcWriter, timestamp(baseTime))
	lastTime := timestamp(msgs[len(msgs)-1].Time)
	writeInt64(crcWriter, int64(lastTime))
	writeInt64(crcWriter, -1)               // default producer id for now
	writeInt16(crcWriter, -1)               // default producer epoch for now
	writeInt32(crcWriter, -1)               // default base sequence
	writeInt32(crcWriter, int32(len(msgs))) // record count

	for _, msg := range msgs {
		writeRecord(crcWriter, CompressionNoneCode, baseTime, baseOffset, msg)
	}
	crcWriter.Flush()

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	crcChecksum := crc32.Checksum(crcBuf.Bytes(), crcTable)

	writeInt32(remainderWriter, int32(crcChecksum))
	remainderWriter.Write(crcBuf.Bytes())

	remainderWriter.Flush()

	writeInt32(bufWriter, int32(remainderBuf.Len()))
	bufWriter.Write(remainderBuf.Bytes())
	bufWriter.Flush()

	return buf.Bytes(), nil
}

var maxDate time.Time = time.Date(5000, time.January, 0, 0, 0, 0, 0, time.UTC)

func baseTime(msgs ...Message) (baseTime time.Time) {
	baseTime = maxDate
	for _, msg := range msgs {
		if msg.Time.Before(baseTime) {
			baseTime = msg.Time
		}
	}
	return
}

func baseOffset(msgs ...Message) (baseOffset int64) {
	baseOffset = math.MaxInt64
	for _, msg := range msgs {
		if msg.Offset < baseOffset {
			baseOffset = msg.Offset
		}
	}
	return
}

func maxOffsetDelta(baseOffset int64, msgs ...Message) (maxDelta int64) {
	maxDelta = 0
	for _, msg := range msgs {
		curDelta := msg.Offset - baseOffset
		if maxDelta > curDelta {
			maxDelta = curDelta
		}
	}
	return
}

func estimatedRecordSize(msg *Message) (size int32) {
	size += 8 + // length
		1 + // attributes
		8 + // timestamp delta
		8 + // offset delta
		8 + // key length
		int32(len(msg.Key)) +
		8 + // value length
		int32(len(msg.Value))
	for _, h := range msg.Headers {
		size += 8 + // header key length
			int32(len(h.Key)) +
			8 + // header value length
			int32(len(h.Value))
	}
	return
}

func calcRecordSize(msg *Message, timestampDelta int64, offsetDelta int64) (size int) {
	size += 1 + // attributes
		varIntLen(timestampDelta) +
		varIntLen(offsetDelta) +
		varIntLen(int64(len(msg.Key))) +
		len(msg.Key) +
		varIntLen(int64(len(msg.Value))) +
		len(msg.Value) +
		varIntLen(int64(len(msg.Headers)))
	for _, h := range msg.Headers {
		size += varIntLen(int64(len([]byte(h.Key)))) +
			len([]byte(h.Key)) +
			varIntLen(int64(len(h.Value))) +
			len(h.Value)
	}
	return
}

func compress(codec CompressionCodec, msgs ...Message) ([]Message, error) {
	estimatedLen := 0
	for _, msg := range msgs {
		estimatedLen += int(msgSize(msg.Key, msg.Value))
	}
	buf := &bytes.Buffer{}
	buf.Grow(estimatedLen)
	bufWriter := bufio.NewWriter(buf)
	for offset, msg := range msgs {
		writeMessage(bufWriter, int64(offset), CompressionNoneCode, msg.Time, msg.Key, msg.Value)
	}
	bufWriter.Flush()

	compressed, err := codec.Encode(buf.Bytes())
	if err != nil {
		return nil, err
	}

	return []Message{{Value: compressed}}, nil
}

const magicByte = 1 // compatible with kafka 0.10.0.0+

func writeMessage(w *bufio.Writer, offset int64, attributes int8, time time.Time, key, value []byte) {
	timestamp := timestamp(time)
	crc32 := crc32OfMessage(magicByte, attributes, timestamp, key, value)
	size := msgSize(key, value)

	writeInt64(w, offset)
	writeInt32(w, size)
	writeInt32(w, int32(crc32))
	writeInt8(w, magicByte)
	writeInt8(w, attributes)
	writeInt64(w, timestamp)
	writeBytes(w, key)
	writeBytes(w, value)
}

func msgSize(key, value []byte) int32 {
	return 4 + // crc
		1 + // magic byte
		1 + // attributes
		8 + // timestamp
		sizeofBytes(key) +
		sizeofBytes(value)
}

// Messages with magic >2 are called records. This method writes messages using message format 2.
func writeRecord(w *bufio.Writer, attributes int8, baseTime time.Time, baseOffset int64, msg Message) {

	timestampDelta := int64(msg.Time.Sub(baseTime))
	offsetDelta := int64(msg.Offset - baseOffset)

	writeVarInt(w, int64(calcRecordSize(&msg, timestampDelta, offsetDelta)))

	writeInt8(w, attributes)
	writeVarInt(w, timestampDelta)
	writeVarInt(w, offsetDelta)

	writeVarInt(w, int64(len(msg.Key)))
	w.Write(msg.Key)
	writeVarInt(w, int64(len(msg.Value)))
	w.Write(msg.Value)
	writeVarInt(w, int64(len(msg.Headers)))

	for _, h := range msg.Headers {
		writeVarInt(w, int64(len(h.Key)))
		w.Write([]byte(h.Key))
		writeVarInt(w, int64(len(h.Value)))
		w.Write(h.Value)
	}
}
