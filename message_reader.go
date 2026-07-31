package kafka

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sort"
)

type readBytesFunc func(*bufio.Reader, int, int) (int, error)

// messageSetReader processes the messages encoded into a fetch response.
// The response may contain a mix of Record Batches (newer format) and Messages
// (older format).
type messageSetReader struct {
	*readerStack      // used for decompressing compressed messages and record batches
	empty        bool // if true, short circuits messageSetReader methods
	debug        bool // enable debug log messages
	// How many bytes are expected to remain in the response.
	//
	// This is used to detect truncation of the response.
	lengthRemain int

	decompressed *bytes.Buffer

	// abortedTxns holds the transactions the broker reported as aborted in the
	// fetch response, ordered by first offset and consumed front to back as the
	// response is read. abortedProducers holds the producers whose records are
	// currently being dropped: a producer enters the set when the reader
	// reaches the first offset of one of its aborted transactions, and leaves
	// it again when the matching abort marker is read.
	//
	// Brokers only send the list when the fetch asked for ReadCommitted, so
	// under ReadUncommitted both stay empty and nothing is dropped.
	abortedTxns      []abortedTransaction
	abortedProducers map[int64]struct{}

	// lastSkippedOffset is the highest offset consumed by a batch that was
	// hidden from the caller, or -1 if there was none. Those offsets appear in
	// no message, so Batch needs this to resume after them; without it a
	// response ending in a control batch would leave the offset behind the
	// marker and the same batch would be fetched forever.
	lastSkippedOffset int64
}

type readerStack struct {
	reader *bufio.Reader
	remain int
	base   int64
	parent *readerStack
	count  int            // how many messages left in the current message set
	header messagesHeader // the current header for a subset of messages within the set.
}

// messagesHeader describes a set of records. there may be many messagesHeader's in a message set.
type messagesHeader struct {
	firstOffset int64
	length      int32
	crc         int32
	magic       int8
	// v1 composes attributes specific to v0 and v1 message headers
	v1 struct {
		attributes int8
		timestamp  int64
	}
	// v2 composes attributes specific to v2 message headers
	v2 struct {
		leaderEpoch     int32
		attributes      int16
		lastOffsetDelta int32
		firstTimestamp  int64
		lastTimestamp   int64
		producerID      int64
		producerEpoch   int16
		baseSequence    int32
		count           int32
	}
}

// control returns true if the header describes a control batch. Control
// batches hold transaction markers written by the transaction coordinator
// rather than records written by a producer, and the protocol requires that
// they are not exposed to the application. Consumers that do surface them see
// an empty message for every committed transaction.
// See https://kafka.apache.org/documentation/#controlbatch
//
// Only v2 message sets support transactions, so v0 and v1 are never control
// batches.
func (h messagesHeader) control() bool {
	const controlMask = 0x20
	return h.magic == 2 && (h.v2.attributes&controlMask) != 0
}

// transactional returns true if the header describes a batch that a producer
// wrote inside a transaction. Such a batch is only visible to a ReadCommitted
// consumer once the transaction commits, so the reader has to be able to tell
// it apart from an ordinary one.
//
// Only v2 message sets support transactions, so v0 and v1 are never
// transactional.
func (h messagesHeader) transactional() bool {
	const transactionalMask = 0x10
	return h.magic == 2 && (h.v2.attributes&transactionalMask) != 0
}

func (h messagesHeader) compression() (codec CompressionCodec, err error) {
	const compressionCodecMask = 0x07
	var code int8
	switch h.magic {
	case 0, 1:
		code = h.v1.attributes & compressionCodecMask
	case 2:
		code = int8(h.v2.attributes & compressionCodecMask)
	default:
		err = h.badMagic()
		return
	}
	if code != 0 {
		codec, err = resolveCodec(code)
	}
	return
}

func (h messagesHeader) badMagic() error {
	return fmt.Errorf("unsupported magic byte %d in header", h.magic)
}

// newMessageSetReader constructs a reader over the message set of a fetch
// response. aborted is the list of aborted transactions the broker returned
// alongside it, which may be nil; the reader uses it to drop the records those
// transactions produced.
func newMessageSetReader(reader *bufio.Reader, remain int, aborted []abortedTransaction) (*messageSetReader, error) {
	// Brokers return the list in offset order, but consuming it front to back
	// depends on that, so sort rather than assume.
	sort.Slice(aborted, func(i, j int) bool {
		return aborted[i].FirstOffset < aborted[j].FirstOffset
	})
	res := &messageSetReader{
		readerStack: &readerStack{
			reader: reader,
			remain: remain,
		},
		decompressed:      acquireBuffer(),
		abortedTxns:       aborted,
		lastSkippedOffset: -1,
	}
	err := res.readHeader()
	return res, err
}

func (r *messageSetReader) remaining() (remain int) {
	if r.empty {
		return 0
	}
	for s := r.readerStack; s != nil; s = s.parent {
		remain += s.remain
	}
	return
}

func (r *messageSetReader) discard() (err error) {
	switch {
	case r.empty:
	case r.readerStack == nil:
	default:
		// rewind up to the top-most reader b/c it's the only one that's doing
		// actual i/o.  the rest are byte buffers that have been pushed on the stack
		// while reading compressed message sets.
		for r.parent != nil {
			r.readerStack = r.parent
		}
		err = r.discardN(r.remain)
	}
	return
}

func (r *messageSetReader) readMessage(min int64, key readBytesFunc, val readBytesFunc) (
	offset int64, lastOffset int64, timestamp int64, headers []Header, err error) {

	for {
		if r.empty {
			err = RequestTimedOut
			return
		}
		if err = r.readHeader(); err != nil {
			return
		}
		switch r.header.magic {
		case 0, 1:
			offset, timestamp, headers, err = r.readMessageV1(min, key, val)
			// Set an invalid value so that it can be ignored
			lastOffset = -1
		case 2:
			// Two kinds of batch are consumed here instead of being returned:
			// control batches, which hold transaction markers rather than
			// messages, and batches whose transaction the broker told us was
			// aborted. Neither may be exposed to the application, so keep
			// reading until a regular batch or the end of the response.
			r.consumeAbortedTransactionsUpTo(r.header.firstOffset)
			if r.count == 0 {
				// A batch the log cleaner emptied. readHeader consumed it
				// whole and accounted for its offsets; dispatching on its
				// header would consume records of the batch that follows.
				continue
			}
			switch {
			case r.header.control():
				if err = r.skipControlRecordV2(min); err != nil {
					return
				}
				continue
			case r.abortedBatch():
				if err = r.skipBatchV2(); err != nil {
					return
				}
				continue
			}
			offset, lastOffset, timestamp, headers, err = r.readMessageV2(min, key, val)
		default:
			err = r.header.badMagic()
		}
		return
	}
}

// consumeAbortedTransactionsUpTo moves every aborted transaction that starts at
// or before offset into the set of producers whose records are being dropped.
// A producer stays in that set until its abort marker is read, which is what
// makes the records between the two invisible.
//
// The reader calls this with the first offset of each batch before deciding
// what to do with it. Repeat calls for the records of one batch are harmless:
// every entry it could match has already been removed from the list.
func (r *messageSetReader) consumeAbortedTransactionsUpTo(offset int64) {
	for len(r.abortedTxns) > 0 && r.abortedTxns[0].FirstOffset <= offset {
		if r.abortedProducers == nil {
			r.abortedProducers = make(map[int64]struct{})
		}
		r.abortedProducers[r.abortedTxns[0].ProducerID] = struct{}{}
		r.abortedTxns = r.abortedTxns[1:]
	}
}

// abortedBatch reports whether the records of the current batch belong to a
// transaction the broker reported as aborted. Batches written outside a
// transaction are never dropped, even when they sit between two that were.
func (r *messageSetReader) abortedBatch() bool {
	if len(r.abortedProducers) == 0 || !r.header.transactional() {
		return false
	}
	_, aborted := r.abortedProducers[r.header.v2.producerID]
	return aborted
}

// skipControlRecordV2 reads a record from a control batch and discards it.
// discardN satisfies readBytesFunc, so the record is consumed by the same code
// path as a regular one and the reader's bookkeeping is left unchanged. The key
// is kept to identify the marker, which decides both what is logged and whether
// a producer stops being a reason to drop records.
func (r *messageSetReader) skipControlRecordV2(min int64) (err error) {
	// Read the header before consuming the record: doing so can exhaust and pop
	// a reader stack, replacing r.header with the enclosing one.
	producerID := r.header.v2.producerID
	lastOffset := r.batchLastOffset()

	var key []byte
	captureKey := func(br *bufio.Reader, size int, nbytes int) (remain int, err error) {
		key, remain, err = readNewBytes(br, size, nbytes)
		return
	}
	if _, _, _, _, err = r.readMessageV2(min, captureKey, discardN); err != nil {
		return
	}

	marker := controlRecordType(key)
	if marker == controlRecordAbort {
		// The transaction this producer opened is over, so its records stop
		// being dropped. A commit marker needs no equivalent: a producer that
		// committed was never added to the set in the first place.
		delete(r.abortedProducers, producerID)
	}
	r.noteBatchSkipped(lastOffset)

	if r.debug {
		r.log("Skipped %s control record for producerID=%d",
			controlRecordTypeName(marker), producerID)
	}
	return
}

// skipBatchV2 discards the record section of the current batch without decoding
// it, for a batch whose transaction was aborted.
//
// lengthRemain is the batch length minus its 49 header bytes, which is exactly
// that record section, so a compressed batch is discarded in its compressed
// form and never inflated. That only holds before any record of the batch has
// been read: once readMessageV2 has started on a compressed batch it has pushed
// a reader stack holding the decompressed bytes and the accounting no longer
// lines up.
func (r *messageSetReader) skipBatchV2() (err error) {
	if r.count != int(r.header.v2.count) {
		return fmt.Errorf("skipBatchV2 called after %d of %d records were read",
			int(r.header.v2.count)-r.count, r.header.v2.count)
	}
	lastOffset := r.batchLastOffset()
	if err = r.discardN(r.lengthRemain); err != nil {
		return
	}
	if r.debug {
		r.log("Skipped aborted batch of %d records for producerID=%d",
			r.header.v2.count, r.header.v2.producerID)
	}
	r.noteBatchSkipped(lastOffset)
	r.lengthRemain = 0
	r.count = 0
	r.unwindStack()
	return
}

// batchLastOffset returns the offset of the last record of the current v2
// batch.
func (r *messageSetReader) batchLastOffset() int64 {
	return r.header.firstOffset + int64(r.header.v2.lastOffsetDelta)
}

// noteBatchSkipped records that every offset up to lastOffset has been consumed
// by a batch the caller never sees, so that Batch can resume past it.
func (r *messageSetReader) noteBatchSkipped(lastOffset int64) {
	if lastOffset > r.lastSkippedOffset {
		r.lastSkippedOffset = lastOffset
	}
}

// Marker types held in the int16 type field of a control record's key.
const (
	controlRecordAbort   int16 = 0
	controlRecordCommit  int16 = 1
	controlRecordUnknown int16 = -1
)

// controlRecordType returns the type of the marker held in a control record's
// key, which is an int16 version followed by an int16 type. A key of any other
// shape is not a marker this client understands.
func controlRecordType(key []byte) int16 {
	if len(key) != 4 {
		return controlRecordUnknown
	}
	return int16(binary.BigEndian.Uint16(key[2:]))
}

func controlRecordTypeName(t int16) string {
	switch t {
	case controlRecordAbort:
		return "ABORT"
	case controlRecordCommit:
		return "COMMIT"
	default:
		return "unknown"
	}
}

func (r *messageSetReader) readMessageV1(min int64, key readBytesFunc, val readBytesFunc) (
	offset int64, timestamp int64, headers []Header, err error) {

	for r.readerStack != nil {
		if r.remain == 0 {
			r.readerStack = r.parent
			continue
		}
		if err = r.readHeader(); err != nil {
			return
		}
		offset = r.header.firstOffset
		timestamp = r.header.v1.timestamp
		var codec CompressionCodec
		if codec, err = r.header.compression(); err != nil {
			return
		}
		if r.debug {
			r.log("Reading with codec=%T", codec)
		}
		if codec != nil {
			// discard next four bytes...will be -1 to indicate null key
			if err = r.discardN(4); err != nil {
				return
			}

			// read and decompress the contained message set.
			r.decompressed.Reset()
			if err = r.readBytesWith(func(br *bufio.Reader, sz int, n int) (remain int, err error) {
				// x4 as a guess that the average compression ratio is near 75%
				r.decompressed.Grow(4 * n)
				limitReader := io.LimitedReader{R: br, N: int64(n)}
				codecReader := codec.NewReader(&limitReader)
				_, err = r.decompressed.ReadFrom(codecReader)
				remain = sz - (n - int(limitReader.N))
				codecReader.Close()
				return
			}); err != nil {
				return
			}

			// the compressed message's offset will be equal to the offset of
			// the last message in the set.  within the compressed set, the
			// offsets will be relative, so we have to scan through them to
			// get the base offset.  for example, if there are four compressed
			// messages at offsets 10-13, then the container message will have
			// offset 13 and the contained messages will be 0,1,2,3.  the base
			// offset for the container, then is 13-3=10.
			if offset, err = extractOffset(offset, r.decompressed.Bytes()); err != nil {
				return
			}

			// mark the outer message as being read
			r.markRead()

			// then push the decompressed bytes onto the stack.
			r.readerStack = &readerStack{
				// Allocate a buffer of size 0, which gets capped at 16 bytes
				// by the bufio package. We are already reading buffered data
				// here, no need to reserve another 4KB buffer.
				reader: bufio.NewReaderSize(r.decompressed, 0),
				remain: r.decompressed.Len(),
				base:   offset,
				parent: r.readerStack,
			}
			continue
		}

		// adjust the offset in case we're reading compressed messages.  the
		// base will be zero otherwise.
		offset += r.base

		// When the messages are compressed kafka may return messages at an
		// earlier offset than the one that was requested, it's the client's
		// responsibility to ignore those.
		//
		// At this point, the message header has been read, so discarding
		// the rest of the message means we have to discard the key, and then
		// the value. Each of those are preceded by a 4-byte length. Discarding
		// them is then reading that length variable and then discarding that
		// amount.
		if offset < min {
			// discard the key
			if err = r.discardBytes(); err != nil {
				return
			}
			// discard the value
			if err = r.discardBytes(); err != nil {
				return
			}
			// since we have fully consumed the message, mark as read
			r.markRead()
			continue
		}
		if err = r.readBytesWith(key); err != nil {
			return
		}
		if err = r.readBytesWith(val); err != nil {
			return
		}
		r.markRead()
		return
	}
	err = errShortRead
	return
}

func (r *messageSetReader) readMessageV2(_ int64, key readBytesFunc, val readBytesFunc) (
	offset int64, lastOffset int64, timestamp int64, headers []Header, err error) {
	if err = r.readHeader(); err != nil {
		return
	}
	if r.count == int(r.header.v2.count) { // first time reading this set, so check for compression headers.
		var codec CompressionCodec
		if codec, err = r.header.compression(); err != nil {
			return
		}
		if codec != nil {
			batchRemain := int(r.header.length - 49) // TODO: document this magic number
			if batchRemain > r.remain {
				err = errShortRead
				return
			}
			if batchRemain < 0 {
				err = fmt.Errorf("batch remain < 0 (%d)", batchRemain)
				return
			}
			r.decompressed.Reset()
			// x4 as a guess that the average compression ratio is near 75%
			r.decompressed.Grow(4 * batchRemain)
			limitReader := io.LimitedReader{R: r.reader, N: int64(batchRemain)}
			codecReader := codec.NewReader(&limitReader)
			_, err = r.decompressed.ReadFrom(codecReader)
			codecReader.Close()
			if err != nil {
				return
			}
			r.remain -= batchRemain - int(limitReader.N)
			r.readerStack = &readerStack{
				reader: bufio.NewReaderSize(r.decompressed, 0), // the new stack reads from the decompressed buffer
				remain: r.decompressed.Len(),
				base:   -1, // base is unused here
				parent: r.readerStack,
				header: r.header,
				count:  r.count,
			}
			// all of the messages in this set are in the decompressed set just pushed onto the reader
			// stack. here we set the parent count to 0 so that when the child set is exhausted, the
			// reader will then try to read the header of the next message set
			r.readerStack.parent.count = 0
		}
	}
	remainBefore := r.remain
	var length int64
	if err = r.readVarInt(&length); err != nil {
		return
	}
	lengthOfLength := remainBefore - r.remain
	var attrs int8
	if err = r.readInt8(&attrs); err != nil {
		return
	}
	var timestampDelta int64
	if err = r.readVarInt(&timestampDelta); err != nil {
		return
	}
	timestamp = r.header.v2.firstTimestamp + timestampDelta
	var offsetDelta int64
	if err = r.readVarInt(&offsetDelta); err != nil {
		return
	}
	offset = r.header.firstOffset + offsetDelta
	if err = r.runFunc(key); err != nil {
		return
	}
	if err = r.runFunc(val); err != nil {
		return
	}
	var headerCount int64
	if err = r.readVarInt(&headerCount); err != nil {
		return
	}
	if headerCount > 0 {
		headers = make([]Header, headerCount)
		for i := range headers {
			if err = r.readMessageHeader(&headers[i]); err != nil {
				return
			}
		}
	}
	lastOffset = r.header.firstOffset + int64(r.header.v2.lastOffsetDelta)
	r.lengthRemain -= int(length) + lengthOfLength
	r.markRead()
	return
}

func (r *messageSetReader) discardBytes() (err error) {
	r.remain, err = discardBytes(r.reader, r.remain)
	return
}

func (r *messageSetReader) discardN(sz int) (err error) {
	r.remain, err = discardN(r.reader, r.remain, sz)
	return
}

func (r *messageSetReader) markRead() {
	if r.count == 0 {
		panic("markRead: negative count")
	}
	r.count--
	r.unwindStack()
	if r.debug {
		r.log("Mark read remain=%d", r.remain)
	}
}

func (r *messageSetReader) unwindStack() {
	for r.count == 0 {
		if r.remain == 0 {
			if r.parent != nil {
				if r.debug {
					r.log("Popped reader stack")
				}
				r.readerStack = r.parent
				continue
			}
		}
		break
	}
}

func (r *messageSetReader) readMessageHeader(header *Header) (err error) {
	var keyLen int64
	if err = r.readVarInt(&keyLen); err != nil {
		return
	}
	if header.Key, err = r.readNewString(int(keyLen)); err != nil {
		return
	}
	var valLen int64
	if err = r.readVarInt(&valLen); err != nil {
		return
	}
	if header.Value, err = r.readNewBytes(int(valLen)); err != nil {
		return
	}
	return nil
}

func (r *messageSetReader) runFunc(rbFunc readBytesFunc) (err error) {
	var length int64
	if err = r.readVarInt(&length); err != nil {
		return
	}
	if r.remain, err = rbFunc(r.reader, r.remain, int(length)); err != nil {
		return
	}
	return
}

func (r *messageSetReader) readHeader() (err error) {
	if r.count > 0 {
		// currently reading a set of messages, no need to read a header until they are exhausted.
		return
	}
	r.header = messagesHeader{}
	if err = r.readInt64(&r.header.firstOffset); err != nil {
		return
	}
	if err = r.readInt32(&r.header.length); err != nil {
		return
	}
	var crcOrLeaderEpoch int32
	if err = r.readInt32(&crcOrLeaderEpoch); err != nil {
		return
	}
	if err = r.readInt8(&r.header.magic); err != nil {
		return
	}
	switch r.header.magic {
	case 0:
		r.header.crc = crcOrLeaderEpoch
		if err = r.readInt8(&r.header.v1.attributes); err != nil {
			return
		}
		r.count = 1
		// Set arbitrary non-zero length so that we always assume the
		// message is truncated since bytes remain.
		r.lengthRemain = 1
		if r.debug {
			r.log("Read v0 header with offset=%d len=%d magic=%d attributes=%d", r.header.firstOffset, r.header.length, r.header.magic, r.header.v1.attributes)
		}
	case 1:
		r.header.crc = crcOrLeaderEpoch
		if err = r.readInt8(&r.header.v1.attributes); err != nil {
			return
		}
		if err = r.readInt64(&r.header.v1.timestamp); err != nil {
			return
		}
		r.count = 1
		// Set arbitrary non-zero length so that we always assume the
		// message is truncated since bytes remain.
		r.lengthRemain = 1
		if r.debug {
			r.log("Read v1 header with remain=%d offset=%d magic=%d and attributes=%d", r.remain, r.header.firstOffset, r.header.magic, r.header.v1.attributes)
		}
	case 2:
		r.header.v2.leaderEpoch = crcOrLeaderEpoch
		if err = r.readInt32(&r.header.crc); err != nil {
			return
		}
		if err = r.readInt16(&r.header.v2.attributes); err != nil {
			return
		}
		if err = r.readInt32(&r.header.v2.lastOffsetDelta); err != nil {
			return
		}
		if err = r.readInt64(&r.header.v2.firstTimestamp); err != nil {
			return
		}
		if err = r.readInt64(&r.header.v2.lastTimestamp); err != nil {
			return
		}
		if err = r.readInt64(&r.header.v2.producerID); err != nil {
			return
		}
		if err = r.readInt16(&r.header.v2.producerEpoch); err != nil {
			return
		}
		if err = r.readInt32(&r.header.v2.baseSequence); err != nil {
			return
		}
		if err = r.readInt32(&r.header.v2.count); err != nil {
			return
		}
		r.count = int(r.header.v2.count)
		// Subtracts the header bytes from the length
		r.lengthRemain = int(r.header.length) - 49
		// The log cleaner can remove every record a batch held and retain the
		// batch itself to preserve the producer's state, so a batch with no
		// records at all is a normal sight on a compacted topic. Reading its
		// header consumed the whole batch, and the next call replaces the
		// header, so the offsets it spans have to be accounted for now: no
		// message will ever surface them.
		if r.count == 0 {
			r.noteBatchSkipped(r.batchLastOffset())
			if r.lengthRemain > 0 {
				if err = r.discardN(r.lengthRemain); err != nil {
					return
				}
			}
			r.lengthRemain = 0
		}
		if r.debug {
			r.log("Read v2 header with count=%d offset=%d len=%d magic=%d attributes=%d", r.count, r.header.firstOffset, r.header.length, r.header.magic, r.header.v2.attributes)
		}
	default:
		err = r.header.badMagic()
		return
	}
	return
}

func (r *messageSetReader) readNewBytes(len int) (res []byte, err error) {
	res, r.remain, err = readNewBytes(r.reader, r.remain, len)
	return
}

func (r *messageSetReader) readNewString(len int) (res string, err error) {
	res, r.remain, err = readNewString(r.reader, r.remain, len)
	return
}

func (r *messageSetReader) readInt8(val *int8) (err error) {
	r.remain, err = readInt8(r.reader, r.remain, val)
	return
}

func (r *messageSetReader) readInt16(val *int16) (err error) {
	r.remain, err = readInt16(r.reader, r.remain, val)
	return
}

func (r *messageSetReader) readInt32(val *int32) (err error) {
	r.remain, err = readInt32(r.reader, r.remain, val)
	return
}

func (r *messageSetReader) readInt64(val *int64) (err error) {
	r.remain, err = readInt64(r.reader, r.remain, val)
	return
}

func (r *messageSetReader) readVarInt(val *int64) (err error) {
	r.remain, err = readVarInt(r.reader, r.remain, val)
	return
}

func (r *messageSetReader) readBytesWith(fn readBytesFunc) (err error) {
	r.remain, err = readBytesWith(r.reader, r.remain, fn)
	return
}

func (r *messageSetReader) log(msg string, args ...interface{}) {
	log.Printf("[DEBUG] "+msg, args...)
}

func extractOffset(base int64, msgSet []byte) (offset int64, err error) {
	r, remain := bufio.NewReader(bytes.NewReader(msgSet)), len(msgSet)
	for remain > 0 {
		if remain, err = readInt64(r, remain, &offset); err != nil {
			return
		}
		var sz int32
		if remain, err = readInt32(r, remain, &sz); err != nil {
			return
		}
		if remain, err = discardN(r, remain, int(sz)); err != nil {
			return
		}
	}
	offset = base - offset
	return
}
