package kafka

import (
	"bufio"
	"encoding/binary"
	"fmt"
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
	case writable:
		v.writeTo(w)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

// This function is used as an optimization to avoid dynamic memory allocations
// in the common case of reading an offset on a single topic and partition.
func writeListOffsetRequestV1(w *bufio.Writer, correleationID int32, clientID string, topic string, parition int32, time int64) error {
	h := requestHeader{
		ApiKey:        int16(listOffsetRequest),
		ApiVersion:    int16(v1),
		CorrelationID: correleationID,
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
	writeInt32(w, parition)
	writeInt64(w, time)

	return w.Flush()
}

// This function is used as an optimization to avoid dynamic memory allocations
// in the common case of sending a batch messages to a kafka server for a single
// topic and partition.
func writeProduceRequestV2(w *bufio.Writer, correleationID int32, clientID string, topic string, partition int32, timeout time.Duration, msgs ...Message) error {
	var size int32

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

	h := requestHeader{
		ApiKey:        int16(produceRequest),
		ApiVersion:    int16(v2),
		CorrelationID: correleationID,
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
	writeInt16(w, -1) // required acks
	writeInt32(w, milliseconds(timeout))

	// topic array
	writeArrayLen(w, 1)
	writeString(w, topic)

	// partition array
	writeArrayLen(w, 1)
	writeInt32(w, partition)
	writeInt32(w, size)

	const magicByte = 1
	const attributes = 0

	for _, msg := range msgs {
		timestamp := timestamp(msg.Time)
		crc32 := crc32OfMessage(magicByte, attributes, timestamp, msg.Key, msg.Value)
		size := 4 + // crc
			1 + // magic byte
			1 + // attributes
			8 + // timestamp
			sizeofBytes(msg.Key) +
			sizeofBytes(msg.Value)

		writeInt64(w, msg.Offset)
		writeInt32(w, int32(size))
		writeInt32(w, int32(crc32))
		writeInt8(w, magicByte)
		writeInt8(w, attributes)
		writeInt64(w, timestamp)
		writeBytes(w, msg.Key)
		writeBytes(w, msg.Value)
	}

	return w.Flush()
}
