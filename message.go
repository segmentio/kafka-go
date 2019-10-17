package kafka

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"time"
)

// Message is a data structure representing kafka messages.
type Message struct {
	// Topic is reads only and MUST NOT be set when writing messages
	Topic string

	// Partition is reads only and MUST NOT be set when writing messages
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header

	// If not set at the creation, Time will be automatically set when
	// writing the message.
	Time time.Time
}

func (msg Message) message(cw *crc32Writer) message {
	m := message{
		MagicByte: 1,
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: timestamp(msg.Time),
	}
	if cw != nil {
		m.CRC = m.crc32(cw)
	}
	return m
}

const timestampSize = 8

func (msg Message) size() int32 {
	return 4 + 1 + 1 + sizeofBytes(msg.Key) + sizeofBytes(msg.Value) + timestampSize
}

type message struct {
	CRC        int32
	MagicByte  int8
	Attributes int8
	Timestamp  int64
	Key        []byte
	Value      []byte
}

func (m message) crc32(cw *crc32Writer) int32 {
	cw.crc32 = 0
	cw.writeInt8(m.MagicByte)
	cw.writeInt8(m.Attributes)
	if m.MagicByte != 0 {
		cw.writeInt64(m.Timestamp)
	}
	cw.writeBytes(m.Key)
	cw.writeBytes(m.Value)
	return int32(cw.crc32)
}

func (m message) size() int32 {
	size := 4 + 1 + 1 + sizeofBytes(m.Key) + sizeofBytes(m.Value)
	if m.MagicByte != 0 {
		size += timestampSize
	}
	return size
}

func (m message) writeTo(wb *writeBuffer) {
	wb.writeInt32(m.CRC)
	wb.writeInt8(m.MagicByte)
	wb.writeInt8(m.Attributes)
	if m.MagicByte != 0 {
		wb.writeInt64(m.Timestamp)
	}
	wb.writeBytes(m.Key)
	wb.writeBytes(m.Value)
}

type messageSetItem struct {
	Offset      int64
	MessageSize int32
	Message     message
}

func (m messageSetItem) size() int32 {
	return 8 + 4 + m.Message.size()
}

func (m messageSetItem) writeTo(wb *writeBuffer) {
	wb.writeInt64(m.Offset)
	wb.writeInt32(m.MessageSize)
	m.Message.writeTo(wb)
}

type messageSet []messageSetItem

func (s messageSet) size() (size int32) {
	for _, m := range s {
		size += m.size()
	}
	return
}

func (s messageSet) writeTo(wb *writeBuffer) {
	for _, m := range s {
		m.writeTo(wb)
	}
}

type messageSetReader struct {
	version int
	v1      messageSetReaderV1
	v2      messageSetReaderV2
}

func (r *messageSetReader) readMessage(min int64,
	key func(*readBuffer, int),
	val func(*readBuffer, int),
) (offset int64, timestamp int64, headers []Header, err error) {
	switch r.version {
	case 0: // empty
		return 0, 0, nil, RequestTimedOut
	case 1:
		return r.v1.readMessage(min, key, val)
	case 2:
		return r.v2.readMessage(min, key, val)
	default:
		panic("Invalid messageSetReader - unknown message reader version")
	}
}

func (r *messageSetReader) done() bool {
	switch r.version {
	case 0: // empty
		return true
	case 1:
		return r.v1.done()
	case 2:
		return r.v2.done()
	default:
		panic("Invalid messageSetReader - unknown message reader version")
	}
}

func (r *messageSetReader) discard() error {
	switch r.version {
	case 0: // empty
		return nil
	case 1:
		return r.v1.discard()
	case 2:
		return r.v2.discard()
	default:
		panic("Invalid messageSetReader - unknown message reader version")
	}
}

type messageSetReaderV1 struct {
	*readerStack
}

type readerStack struct {
	parent *readerStack
	reader *readBuffer
	base   int64
}

func (r *readerStack) rewind() *readerStack {
	for r != nil && r.parent != nil {
		r = r.parent
	}
	return r
}

func (r *readerStack) discard() error {
	r = r.rewind()

	if r.reader != nil {
		r.reader.discardAll()
		if err := r.reader.err; err != errShortRead {
			return err
		}
	}

	return nil
}

func newMessageSetReader(rb *readBuffer, remain int) (*messageSetReader, error) {
	const headerLength = 8 + 4 + 4 + 1 // offset + messageSize + crc + magicByte

	if headerLength > rb.n {
		return nil, errShortRead
	}

	b, err := rb.r.(*bufio.Reader).Peek(headerLength)
	if err != nil {
		return nil, err
	}

	version := b[headerLength-1]
	switch version {
	case 0, 1:
		mr := &messageSetReader{
			version: 1,
			v1: messageSetReaderV1{
				readerStack: &readerStack{reader: rb},
			},
		}
		return mr, nil
	case 2:
		mr := &messageSetReader{
			version: 2,
			v2: messageSetReaderV2{
				readerStack: &readerStack{reader: rb},
			},
		}
		return mr, nil
	default:
		return nil, fmt.Errorf("unsupported message version %d found in fetch response", version)
	}
}

func (r *messageSetReaderV1) readMessage(min int64,
	key func(*readBuffer, int),
	val func(*readBuffer, int),
) (offset int64, timestamp int64, headers []Header, err error) {
	for r.readerStack != nil {
		if r.reader.n == 0 {
			r.readerStack = r.parent
			continue
		}

		var attributes int8
		if offset, attributes, timestamp, err = r.reader.readMessageHeader(); err != nil {
			return
		}

		// if the message is compressed, decompress it and push a new reader
		// onto the stack.
		code := attributes & compressionCodecMask
		if code != 0 {
			var codec CompressionCodec
			if codec, err = resolveCodec(code); err != nil {
				return
			}

			// discard next four bytes...will be -1 to indicate null key
			r.reader.discard(4)

			// read and decompress the contained message set.
			var decompressed bytes.Buffer
			var nbytes = r.reader.readInt32()
			// 4x as a guess that the average compression ratio is near 75%
			decompressed.Grow(4 * int(nbytes))

			l := io.LimitedReader{R: r.reader, N: int64(nbytes)}
			d := codec.NewReader(&l)

			_, err = decompressed.ReadFrom(d)
			d.Close()
			if err != nil {
				return
			}
			io.Copy(ioutil.Discard, &l)

			// the compressed message's offset will be equal to the offset of
			// the last message in the set.  within the compressed set, the
			// offsets will be relative, so we have to scan through them to
			// get the base offset.  for example, if there are four compressed
			// messages at offsets 10-13, then the container message will have
			// offset 13 and the contained messages will be 0,1,2,3.  the base
			// offset for the container, then is 13-3=10.
			if offset, err = extractOffset(offset, decompressed.Bytes()); err != nil {
				return
			}

			r.readerStack = &readerStack{
				parent: r.readerStack,
				reader: &readBuffer{
					r: &decompressed,
					n: decompressed.Len(),
				},
				base: offset,
			}
			continue
		}

		rb := r.reader
		// adjust the offset in case we're reading compressed messages.  the
		// base will be zero otherwise.
		offset += r.base

		// When the messages are compressed kafka may return messages at an
		// earlier offset than the one that was requested, it's the client's
		// responsibility to ignore those.
		if offset < min {
			rb.discardBytes()
			rb.discardBytes()
			continue
		}

		if n := int(rb.readInt32()); n >= 0 {
			key(rb, n)
		}

		if n := int(rb.readInt32()); n >= 0 {
			val(rb, n)
		}

		err = rb.err
		return
	}

	err = errShortRead
	return
}

func (r *messageSetReaderV1) done() bool {
	return r.readerStack == nil
}

func (r *messageSetReaderV1) discard() error {
	rs := r.readerStack
	r.readerStack = nil

	if rs != nil {
		return rs.discard()
	}

	return nil
}

func extractOffset(base int64, msgSet []byte) (offset int64, err error) {
	for b := msgSet; len(b) > 0; {
		if len(b) < 12 {
			err = errShortRead
			return
		}

		offset = makeInt64(b[:8])
		size := makeInt32(b[8:12])

		if len(b) < int(12+size) {
			err = errShortRead
			return
		}

		b = b[12+size:]
	}
	offset = base - offset
	return
}

type Header struct {
	Key   string
	Value []byte
}

type messageSetHeaderV2 struct {
	firstOffset          int64
	length               int32
	partitionLeaderEpoch int32
	magic                int8
	crc                  int32
	batchAttributes      int16
	lastOffsetDelta      int32
	firstTimestamp       int64
	maxTimestamp         int64
	producerId           int64
	producerEpoch        int16
	firstSequence        int32
}

type timestampType int8

const (
	createTime    timestampType = 0
	logAppendTime timestampType = 1
)

type transactionType int8

const (
	nonTransactional transactionType = 0
	transactional    transactionType = 1
)

type controlType int8

const (
	nonControlMessage controlType = 0
	controlMessage    controlType = 1
)

func (h *messageSetHeaderV2) compression() int8 {
	return int8(h.batchAttributes & 7)
}

func (h *messageSetHeaderV2) timestampType() timestampType {
	return timestampType((h.batchAttributes & (1 << 3)) >> 3)
}

func (h *messageSetHeaderV2) transactionType() transactionType {
	return transactionType((h.batchAttributes & (1 << 4)) >> 4)
}

func (h *messageSetHeaderV2) controlType() controlType {
	return controlType((h.batchAttributes & (1 << 5)) >> 5)
}

type messageSetReaderV2 struct {
	*readerStack
	messageCount int

	header messageSetHeaderV2
}

func (r *messageSetReaderV2) readHeader() error {
	rb := r.reader

	r.header = messageSetHeaderV2{
		firstOffset:          rb.readInt64(),
		length:               rb.readInt32(),
		partitionLeaderEpoch: rb.readInt32(),
		magic:                rb.readInt8(),
		crc:                  rb.readInt32(),
		batchAttributes:      rb.readInt16(),
		lastOffsetDelta:      rb.readInt32(),
		firstTimestamp:       rb.readInt64(),
		maxTimestamp:         rb.readInt64(),
		producerId:           rb.readInt64(),
		producerEpoch:        rb.readInt16(),
		firstSequence:        rb.readInt32(),
	}

	r.messageCount = int(rb.readInt32())
	return rb.err
}

func (r *messageSetReaderV2) readMessage(min int64,
	key func(*readBuffer, int),
	val func(*readBuffer, int),
) (offset int64, timestamp int64, headers []Header, err error) {
	if r.messageCount == 0 {
		if r.reader.n == 0 {
			if r.parent != nil {
				r.readerStack = r.parent
			}
		}

		if err = r.readHeader(); err != nil {
			return
		}

		if code := r.header.compression(); code != 0 {
			var codec CompressionCodec
			if codec, err = resolveCodec(code); err != nil {
				return
			}

			var batchRemain = int(r.header.length - 49)
			if batchRemain > r.reader.n {
				err = errShortRead
				return
			}

			var decompressed bytes.Buffer
			decompressed.Grow(4 * batchRemain)

			l := io.LimitedReader{R: r.reader, N: int64(batchRemain)}
			d := codec.NewReader(&l)

			_, err = decompressed.ReadFrom(d)
			d.Close()
			if err != nil {
				return
			}
			io.Copy(ioutil.Discard, &l)

			r.readerStack = &readerStack{
				parent: r.readerStack,
				reader: &readBuffer{
					r: &decompressed,
					n: decompressed.Len(),
				},
				base: -1, // base is unused here
			}
		}
	}

	rb := r.reader
	_ = rb.readVarInt() // length
	_ = rb.readInt8()   // attributes
	timestampDelta := rb.readVarInt()
	offsetDelta := rb.readVarInt()

	if n := int(rb.readVarInt()); n >= 0 {
		key(rb, n)
	}

	if n := int(rb.readVarInt()); n >= 0 {
		val(rb, n)
	}

	headerCount := rb.readVarInt()
	headers = make([]Header, headerCount)

	for i := 0; i < int(headerCount); i++ {
		headers[i] = Header{
			Key:   rb.readVarString(),
			Value: rb.readVarBytes(),
		}
	}

	r.messageCount--
	return r.header.firstOffset + offsetDelta, r.header.firstTimestamp + timestampDelta, headers, rb.err
}

func (r *messageSetReaderV2) done() bool {
	return r.messageCount == 0
}

func (r *messageSetReaderV2) discard() error {
	rs := r.readerStack
	r.readerStack = nil
	r.messageCount = 0

	if rs != nil {
		return rs.discard()
	}

	return nil
}
