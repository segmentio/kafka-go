package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

type writeBuffer struct {
	w io.Writer
	b [16]byte
}

func (wb *writeBuffer) writeInt8(i int8) {
	wb.b[0] = byte(i)
	wb.Write(wb.b[:1])
}

func (wb *writeBuffer) writeInt16(i int16) {
	binary.BigEndian.PutUint16(wb.b[:2], uint16(i))
	wb.Write(wb.b[:2])
}

func (wb *writeBuffer) writeInt32(i int32) {
	binary.BigEndian.PutUint32(wb.b[:4], uint32(i))
	wb.Write(wb.b[:4])
}

func (wb *writeBuffer) writeInt64(i int64) {
	binary.BigEndian.PutUint64(wb.b[:8], uint64(i))
	wb.Write(wb.b[:8])
}

func (wb *writeBuffer) writeVarInt(i int64) {
	u := uint64((i << 1) ^ (i >> 63))
	n := 0

	for u >= 0x80 && n < len(wb.b) {
		wb.b[n] = byte(u) | 0x80
		u >>= 7
		n++
	}

	if n < len(wb.b) {
		wb.b[n] = byte(u)
		n++
	}

	wb.Write(wb.b[:n])
}

func (wb *writeBuffer) writeString(s string) {
	wb.writeInt16(int16(len(s)))
	wb.WriteString(s)
}

func (wb *writeBuffer) writeVarString(s string) {
	wb.writeVarInt(int64(len(s)))
	wb.WriteString(s)
}

func (wb *writeBuffer) writeNullableString(s *string) {
	if s == nil {
		wb.writeInt16(-1)
	} else {
		wb.writeString(*s)
	}
}

func (wb *writeBuffer) writeBytes(b []byte) {
	n := len(b)
	if b == nil {
		n = -1
	}
	wb.writeInt32(int32(n))
	wb.Write(b)
}

func (wb *writeBuffer) writeVarBytes(b []byte) {
	wb.writeVarInt(int64(len(b)))
	wb.Write(b)
}

func (wb *writeBuffer) writeBool(b bool) {
	v := int8(0)
	if b {
		v = 1
	}
	wb.writeInt8(v)
}

func (wb *writeBuffer) writeArrayLen(n int) {
	wb.writeInt32(int32(n))
}

func (wb *writeBuffer) writeArray(n int, f func(int)) {
	wb.writeArrayLen(n)
	for i := 0; i < n; i++ {
		f(i)
	}
}

func (wb *writeBuffer) writeVarArray(n int, f func(int)) {
	wb.writeVarInt(int64(n))
	for i := 0; i < n; i++ {
		f(i)
	}
}

func (wb *writeBuffer) writeStringArray(a []string) {
	wb.writeArray(len(a), func(i int) { wb.writeString(a[i]) })
}

func (wb *writeBuffer) writeInt32Array(a []int32) {
	wb.writeArray(len(a), func(i int) { wb.writeInt32(a[i]) })
}

func (wb *writeBuffer) write(a interface{}) {
	switch v := a.(type) {
	case int8:
		wb.writeInt8(v)
	case int16:
		wb.writeInt16(v)
	case int32:
		wb.writeInt32(v)
	case int64:
		wb.writeInt64(v)
	case string:
		wb.writeString(v)
	case []byte:
		wb.writeBytes(v)
	case bool:
		wb.writeBool(v)
	case writable:
		v.writeTo(wb)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func (wb *writeBuffer) Write(b []byte) (int, error) {
	return wb.w.Write(b)
}

func (wb *writeBuffer) WriteString(s string) (int, error) {
	return io.WriteString(wb.w, s)
}

func (wb *writeBuffer) Flush() error {
	if x, ok := wb.w.(interface{ Flush() error }); ok {
		return x.Flush()
	}
	return nil
}

type writable interface {
	writeTo(*writeBuffer)
}

func (wb *writeBuffer) writeFetchRequestV2(correlationID int32, clientID, topic string, partition int32, offset int64, minBytes, maxBytes int, maxWait time.Duration) error {
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

	h.writeTo(wb)
	wb.writeInt32(-1) // replica ID
	wb.writeInt32(milliseconds(maxWait))
	wb.writeInt32(int32(minBytes))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)
	wb.writeInt64(offset)
	wb.writeInt32(int32(maxBytes))

	return wb.Flush()
}

func (wb *writeBuffer) writeFetchRequestV5(correlationID int32, clientID, topic string, partition int32, offset int64, minBytes, maxBytes int, maxWait time.Duration, isolationLevel int8) error {
	h := requestHeader{
		ApiKey:        int16(fetchRequest),
		ApiVersion:    int16(v5),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // max wait time
		4 + // min bytes
		4 + // max bytes
		1 + // isolation level
		4 + // topic array length
		sizeofString(topic) +
		4 + // partition array length
		4 + // partition
		8 + // offset
		8 + // log start offset
		4 // max bytes

	h.writeTo(wb)
	wb.writeInt32(-1) // replica ID
	wb.writeInt32(milliseconds(maxWait))
	wb.writeInt32(int32(minBytes))
	wb.writeInt32(int32(maxBytes))
	wb.writeInt8(isolationLevel) // isolation level 0 - read uncommitted

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)
	wb.writeInt64(offset)
	wb.writeInt64(int64(0)) // log start offset only used when is sent by follower
	wb.writeInt32(int32(maxBytes))

	return wb.Flush()
}

func (wb *writeBuffer) writeFetchRequestV10(correlationID int32, clientID, topic string, partition int32, offset int64, minBytes, maxBytes int, maxWait time.Duration, isolationLevel int8) error {
	h := requestHeader{
		ApiKey:        int16(fetchRequest),
		ApiVersion:    int16(v10),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // max wait time
		4 + // min bytes
		4 + // max bytes
		1 + // isolation level
		4 + // session ID
		4 + // session epoch
		4 + // topic array length
		sizeofString(topic) +
		4 + // partition array length
		4 + // partition
		4 + // current leader epoch
		8 + // fetch offset
		8 + // log start offset
		4 + // partition max bytes
		4 // forgotten topics data

	h.writeTo(wb)
	wb.writeInt32(-1) // replica ID
	wb.writeInt32(milliseconds(maxWait))
	wb.writeInt32(int32(minBytes))
	wb.writeInt32(int32(maxBytes))
	wb.writeInt8(isolationLevel) // isolation level 0 - read uncommitted
	wb.writeInt32(0)             //FIXME
	wb.writeInt32(-1)            //FIXME

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)
	wb.writeInt32(-1) //FIXME
	wb.writeInt64(offset)
	wb.writeInt64(int64(0)) // log start offset only used when is sent by follower
	wb.writeInt32(int32(maxBytes))

	// forgotten topics array
	wb.writeArrayLen(0) // forgotten topics not supported yet

	return wb.Flush()
}

func (wb *writeBuffer) writeListOffsetRequestV1(correlationID int32, clientID, topic string, partition int32, time int64) error {
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

	h.writeTo(wb)
	wb.writeInt32(-1) // replica ID

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)
	wb.writeInt64(time)

	return wb.Flush()
}

func (wb *writeBuffer) writeProduceRequestV2(codec CompressionCodec, correlationID int32, clientID, topic string, partition int32, timeout time.Duration, requiredAcks int16, msgs ...Message) (err error) {
	attributes := int8(CompressionNoneCode)
	if codec != nil {
		if msgs, err = compressMessageSet(codec, msgs...); err != nil {
			return err
		}
		attributes = codec.Code()
	}
	size := messageSetSize(msgs...)

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

	h.writeTo(wb)
	wb.writeInt16(requiredAcks) // required acks
	wb.writeInt32(milliseconds(timeout))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)

	wb.writeInt32(size)
	cw := &crc32Writer{table: crc32.IEEETable}

	for _, msg := range msgs {
		wb.writeMessage(msg.Offset, attributes, msg.Time, msg.Key, msg.Value, cw)
	}

	return wb.Flush()
}

func (wb *writeBuffer) writeProduceRequestV3(codec CompressionCodec, correlationID int32, clientID, topic string, partition int32, timeout time.Duration, requiredAcks int16, transactionalID *string, msgs ...Message) (err error) {
	var size int32
	var compressed []byte
	var attributes int16

	if codec == nil {
		size = recordBatchSize(msgs...)
	} else {
		compressed, attributes, size, err = compressRecordBatch(codec, msgs...)
		if err != nil {
			return
		}
	}

	h := requestHeader{
		ApiKey:        int16(produceRequest),
		ApiVersion:    int16(v3),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}

	h.Size = (h.size() - 4) +
		sizeofNullableString(transactionalID) +
		2 + // required acks
		4 + // timeout
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		4 + // message set size
		size

	h.writeTo(wb)
	wb.writeNullableString(transactionalID)
	wb.writeInt16(requiredAcks) // required acks
	wb.writeInt32(milliseconds(timeout))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)

	wb.writeInt32(size)
	baseTime := msgs[0].Time
	lastTime := msgs[len(msgs)-1].Time

	if codec != nil {
		wb.writeRecordBatch(attributes, size, len(msgs), baseTime, lastTime, func(wb *writeBuffer) {
			wb.Write(compressed)
		})
	} else {
		wb.writeRecordBatch(attributes, size, len(msgs), baseTime, lastTime, func(wb *writeBuffer) {
			for i, msg := range msgs {
				wb.writeRecord(0, msgs[0].Time, int64(i), msg)
			}
		})
	}

	return wb.Flush()
}

func (wb *writeBuffer) writeProduceRequestV7(codec CompressionCodec, correlationID int32, clientID, topic string, partition int32, timeout time.Duration, requiredAcks int16, transactionalID *string, msgs ...Message) (err error) {
	var size int32
	var compressed []byte
	var attributes int16

	if codec == nil {
		size = recordBatchSize(msgs...)
	} else {
		compressed, attributes, size, err = compressRecordBatch(codec, msgs...)
		if err != nil {
			return
		}
	}

	h := requestHeader{
		ApiKey:        int16(produceRequest),
		ApiVersion:    int16(v7),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		sizeofNullableString(transactionalID) +
		2 + // required acks
		4 + // timeout
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		4 + // message set size
		size

	h.writeTo(wb)
	wb.writeNullableString(transactionalID)
	wb.writeInt16(requiredAcks) // required acks
	wb.writeInt32(milliseconds(timeout))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)

	wb.writeInt32(size)
	baseTime := msgs[0].Time
	lastTime := msgs[len(msgs)-1].Time

	if codec != nil {
		wb.writeRecordBatch(attributes, size, len(msgs), baseTime, lastTime, func(wb *writeBuffer) {
			wb.Write(compressed)
		})
	} else {
		wb.writeRecordBatch(attributes, size, len(msgs), baseTime, lastTime, func(wb *writeBuffer) {
			for i, msg := range msgs {
				wb.writeRecord(0, msgs[0].Time, int64(i), msg)
			}
		})
	}

	return wb.Flush()
}

func (wb *writeBuffer) writeRecordBatch(attributes int16, size int32, count int, baseTime, lastTime time.Time, write func(*writeBuffer)) {
	var (
		baseTimestamp   = timestamp(baseTime)
		lastTimestamp   = timestamp(lastTime)
		lastOffsetDelta = int32(count - 1)
		producerID      = int64(-1)    // default producer id for now
		producerEpoch   = int16(-1)    // default producer epoch for now
		baseSequence    = int32(-1)    // default base sequence
		recordCount     = int32(count) // record count
		writerBackup    = wb.w
	)

	// dry run to compute the checksum
	cw := &crc32Writer{table: crc32.MakeTable(crc32.Castagnoli)}
	wb.w = cw
	cw.writeInt16(attributes) // attributes, timestamp type 0 - create time, not part of a transaction, no control messages
	cw.writeInt32(lastOffsetDelta)
	cw.writeInt64(baseTimestamp)
	cw.writeInt64(lastTimestamp)
	cw.writeInt64(producerID)
	cw.writeInt16(producerEpoch)
	cw.writeInt32(baseSequence)
	cw.writeInt32(recordCount)
	write(wb)
	wb.w = writerBackup

	// actual write to the output buffer
	wb.writeInt64(int64(0))
	wb.writeInt32(int32(size - 12)) // 12 = batch length + base offset sizes
	wb.writeInt32(-1)               // partition leader epoch
	wb.writeInt8(2)                 // magic byte
	wb.writeInt32(int32(cw.crc32))

	wb.writeInt16(attributes)
	wb.writeInt32(lastOffsetDelta)
	wb.writeInt64(baseTimestamp)
	wb.writeInt64(lastTimestamp)
	wb.writeInt64(producerID)
	wb.writeInt16(producerEpoch)
	wb.writeInt32(baseSequence)
	wb.writeInt32(recordCount)
	write(wb)
}

var maxDate = time.Date(5000, time.January, 0, 0, 0, 0, 0, time.UTC)

func compressMessageSet(codec CompressionCodec, msgs ...Message) ([]Message, error) {
	estimatedLen := 0

	for _, msg := range msgs {
		estimatedLen += int(messageSize(msg.Key, msg.Value))
	}

	buffer := &bytes.Buffer{}
	buffer.Grow(estimatedLen / 2)
	compressor := codec.NewWriter(buffer)
	wb := &writeBuffer{w: compressor}
	cw := &crc32Writer{table: crc32.IEEETable}

	for offset, msg := range msgs {
		wb.writeMessage(int64(offset), CompressionNoneCode, msg.Time, msg.Key, msg.Value, cw)
	}

	if err := compressor.Close(); err != nil {
		return nil, err
	}

	return []Message{{Value: buffer.Bytes()}}, nil
}

func compressRecordBatch(codec CompressionCodec, msgs ...Message) (compressed []byte, attributes int16, size int32, err error) {
	recordBuf := &bytes.Buffer{}
	recordBuf.Grow(int(recordBatchSize(msgs...)) / 2)
	compressor := codec.NewWriter(recordBuf)
	wb := &writeBuffer{w: compressor}

	for i, msg := range msgs {
		wb.writeRecord(0, msgs[0].Time, int64(i), msg)
	}

	if err = compressor.Close(); err != nil {
		return
	}

	compressed = recordBuf.Bytes()
	attributes = int16(codec.Code())
	size = recordBatchHeaderSize + int32(len(compressed))
	return
}

func (wb *writeBuffer) writeMessage(offset int64, attributes int8, time time.Time, key, value []byte, cw *crc32Writer) {
	const magicByte = 1 // compatible with kafka 0.10.0.0+

	timestamp := timestamp(time)
	size := messageSize(key, value)

	// dry run to compute the checksum
	cw.crc32 = 0
	cw.writeInt8(magicByte)
	cw.writeInt8(attributes)
	cw.writeInt64(timestamp)
	cw.writeBytes(key)
	cw.writeBytes(value)

	// actual write to the output buffer
	wb.writeInt64(offset)
	wb.writeInt32(size)
	wb.writeInt32(int32(cw.crc32))
	wb.writeInt8(magicByte)
	wb.writeInt8(attributes)
	wb.writeInt64(timestamp)
	wb.writeBytes(key)
	wb.writeBytes(value)
}

// Messages with magic >2 are called records. This method writes messages using message format 2.
func (wb *writeBuffer) writeRecord(attributes int8, baseTime time.Time, offset int64, msg Message) {
	timestampDelta := msg.Time.Sub(baseTime)
	offsetDelta := int64(offset)

	wb.writeVarInt(int64(recordSize(&msg, timestampDelta, offsetDelta)))
	wb.writeInt8(attributes)
	wb.writeVarInt(int64(milliseconds(timestampDelta)))
	wb.writeVarInt(offsetDelta)

	wb.writeVarBytes(msg.Key)
	wb.writeVarBytes(msg.Value)
	wb.writeVarArray(len(msg.Headers), func(i int) {
		h := &msg.Headers[i]
		wb.writeVarString(h.Key)
		wb.writeVarBytes(h.Value)
	})
}

func varIntLen(i int64) int {
	u := uint64((i << 1) ^ (i >> 63)) // zig-zag encoding
	n := 0

	for u >= 0x80 {
		u >>= 7
		n++
	}

	return n + 1
}

func varBytesLen(b []byte) int {
	return varIntLen(int64(len(b))) + len(b)
}

func varStringLen(s string) int {
	return varIntLen(int64(len(s))) + len(s)
}

func varArrayLen(n int, f func(int) int) int {
	size := varIntLen(int64(n))
	for i := 0; i < n; i++ {
		size += f(i)
	}
	return size
}

func messageSize(key, value []byte) int32 {
	return 4 + // crc
		1 + // magic byte
		1 + // attributes
		8 + // timestamp
		sizeofBytes(key) +
		sizeofBytes(value)
}

func messageSetSize(msgs ...Message) (size int32) {
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
	return
}

func recordSize(msg *Message, timestampDelta time.Duration, offsetDelta int64) int {
	return 1 + // attributes
		varIntLen(int64(milliseconds(timestampDelta))) +
		varIntLen(offsetDelta) +
		varBytesLen(msg.Key) +
		varBytesLen(msg.Value) +
		varArrayLen(len(msg.Headers), func(i int) int {
			h := &msg.Headers[i]
			return varStringLen(h.Key) + varBytesLen(h.Value)
		})
}

const recordBatchHeaderSize int32 = 0 +
	8 + // base offset
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
	4 + // base sequence
	4 // msg count

func recordBatchSize(msgs ...Message) (size int32) {
	size = recordBatchHeaderSize
	baseTime := msgs[0].Time

	for i := range msgs {
		msg := &msgs[i]
		msz := recordSize(msg, msg.Time.Sub(baseTime), int64(i))
		size += int32(msz + varIntLen(int64(msz)))
	}

	return
}
