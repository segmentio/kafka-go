package kafka

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/compress/gzip"
)

// appendZigZagVarInt encodes v the way the record format does, matching
// readVarInt's decoding.
func appendZigZagVarInt(b []byte, v int64) []byte {
	u := uint64(v<<1) ^ uint64(v>>63)
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}

// v2Record builds a single record of the v2 message format.
func v2Record(offsetDelta int64, key, value []byte) []byte {
	var body []byte
	body = append(body, 0) // attributes
	body = appendZigZagVarInt(body, 0)
	body = appendZigZagVarInt(body, offsetDelta)
	body = appendZigZagVarInt(body, int64(len(key)))
	body = append(body, key...)
	body = appendZigZagVarInt(body, int64(len(value)))
	body = append(body, value...)
	body = appendZigZagVarInt(body, 0) // header count

	out := appendZigZagVarInt(nil, int64(len(body)))
	return append(out, body...)
}

// batchOpts describes the header fields of a v2 record batch that the reader's
// transaction handling depends on.
type batchOpts struct {
	firstOffset   int64
	producerID    int64
	control       bool
	transactional bool
	codec         CompressionCodec // nil leaves the records uncompressed
}

// v2Batch builds a record batch of the v2 message format. The CRC is left zero
// because the reader does not verify it.
func v2Batch(t *testing.T, o batchOpts, records ...[]byte) []byte {
	t.Helper()

	var recs []byte
	for _, r := range records {
		recs = append(recs, r...)
	}

	var attributes uint16
	if o.control {
		attributes |= 0x20
	}
	if o.transactional {
		attributes |= 0x10
	}
	if o.codec != nil {
		attributes |= uint16(o.codec.Code()) & 0x07

		buf := &bytes.Buffer{}
		w := o.codec.NewWriter(buf)
		if _, err := w.Write(recs); err != nil {
			t.Fatalf("compressing records: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("closing the compression writer: %v", err)
		}
		recs = buf.Bytes()
	}

	b := make([]byte, 0, 61+len(recs))
	b = binary.BigEndian.AppendUint64(b, uint64(o.firstOffset))
	b = binary.BigEndian.AppendUint32(b, uint32(49+len(recs))) // length after this field
	b = binary.BigEndian.AppendUint32(b, 0)                    // partitionLeaderEpoch
	b = append(b, 2)                                           // magic
	b = binary.BigEndian.AppendUint32(b, 0)                    // crc
	b = binary.BigEndian.AppendUint16(b, attributes)
	b = binary.BigEndian.AppendUint32(b, uint32(len(records)-1)) // lastOffsetDelta
	b = binary.BigEndian.AppendUint64(b, 0)                      // firstTimestamp
	b = binary.BigEndian.AppendUint64(b, 0)                      // maxTimestamp
	b = binary.BigEndian.AppendUint64(b, uint64(o.producerID))
	b = binary.BigEndian.AppendUint16(b, 0)                    // producerEpoch
	b = binary.BigEndian.AppendUint32(b, 0)                    // baseSequence
	b = binary.BigEndian.AppendUint32(b, uint32(len(records))) // record count
	return append(b, recs...)
}

// marker builds the record a transaction coordinator writes to close a
// transaction: a key of int16 version and int16 type, and a value of int16
// version and int32 coordinator epoch.
func marker(offsetDelta int64, markerType int16) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint16(key[2:], uint16(markerType))
	return v2Record(offsetDelta, key, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
}

func commitMarker(offsetDelta int64) []byte {
	return marker(offsetDelta, controlRecordCommit)
}

func abortMarker(offsetDelta int64) []byte {
	return marker(offsetDelta, controlRecordAbort)
}

// controlBatch builds the single-record control batch that closes a
// transaction at offset.
func controlBatch(t *testing.T, offset, producerID int64, markerType int16) []byte {
	t.Helper()
	return v2Batch(t,
		batchOpts{firstOffset: offset, producerID: producerID, control: true, transactional: true},
		marker(0, markerType))
}

// emptyBatch builds a record batch whose records the log cleaner removed. The
// cleaner keeps the original offset range when it retains an empty batch, so
// lastOffsetDelta is the one the batch had before cleaning, not the -1 that
// zero records would otherwise imply.
func emptyBatch(t *testing.T, o batchOpts, lastOffsetDelta int32) []byte {
	t.Helper()
	b := v2Batch(t, o)
	binary.BigEndian.PutUint32(b[23:27], uint32(lastOffsetDelta))
	return b
}

func newControlTestReader(t *testing.T, data []byte, aborted ...abortedTransaction) *messageSetReader {
	t.Helper()
	r, err := newMessageSetReader(bufio.NewReader(bytes.NewReader(data)), len(data), aborted)
	if err != nil {
		t.Fatalf("newMessageSetReader: %v", err)
	}
	return r
}

func readOneMessage(r *messageSetReader, offset int64) (msg Message, err error) {
	keyFunc := func(r *bufio.Reader, size int, nbytes int) (remain int, err error) {
		msg.Key, remain, err = readNewBytes(r, size, nbytes)
		return
	}
	valFunc := func(r *bufio.Reader, size int, nbytes int) (remain int, err error) {
		msg.Value, remain, err = readNewBytes(r, size, nbytes)
		return
	}
	msg.Offset, _, _, _, err = r.readMessage(offset, keyFunc, valFunc)
	return
}

// readAllValues drains the reader and returns the value of every message it
// surfaced. Any error other than the exhaustion of the response fails the test.
func readAllValues(t *testing.T, r *messageSetReader) []string {
	t.Helper()

	var values []string
	var offset int64
	for {
		msg, err := readOneMessage(r, offset)
		if err != nil {
			if !errors.Is(err, errShortRead) {
				t.Fatalf("readMessage after %d messages: %v", len(values), err)
			}
			return values
		}
		values = append(values, string(msg.Value))
		offset = msg.Offset + 1
	}
}

// TestReadMessageSkipsLeadingControlBatch verifies that a control batch ahead
// of the data does not surface as a message.
func TestReadMessageSkipsLeadingControlBatch(t *testing.T) {
	data := append(
		controlBatch(t, 0, 0, controlRecordCommit),
		v2Batch(t, batchOpts{firstOffset: 1}, v2Record(0, []byte("key-1"), []byte("value-1")))...,
	)
	r := newControlTestReader(t, data)

	msg, err := readOneMessage(r, 0)
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if string(msg.Key) != "key-1" || string(msg.Value) != "value-1" {
		t.Errorf("expected key-1/value-1, got key=%q value=%q", msg.Key, msg.Value)
	}
	if msg.Offset != 1 {
		t.Errorf("offset = %d, want 1", msg.Offset)
	}
}

// TestReadMessageSkipsInterleavedControlBatches covers the usual layout of a
// transactional producer, which writes a marker after every committed batch.
func TestReadMessageSkipsInterleavedControlBatches(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t, batchOpts{firstOffset: 0}, v2Record(0, []byte("k0"), []byte("v0")))...)
	data = append(data, controlBatch(t, 1, 0, controlRecordCommit)...)
	data = append(data, v2Batch(t, batchOpts{firstOffset: 2}, v2Record(0, []byte("k1"), []byte("v1")))...)
	data = append(data, controlBatch(t, 3, 0, controlRecordCommit)...)
	data = append(data, v2Batch(t, batchOpts{firstOffset: 4}, v2Record(0, []byte("k2"), []byte("v2")))...)

	r := newControlTestReader(t, data)

	if got, want := readAllValues(t, r), []string{"v0", "v1", "v2"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageTrailingControlBatchIsNotAMessage verifies that a response
// ending in a control batch reports exhaustion instead of returning an empty
// message. errShortRead is how the reader signals an exhausted response, and
// Batch.readMessage already translates it into end of batch.
func TestReadMessageTrailingControlBatchIsNotAMessage(t *testing.T) {
	data := append(
		v2Batch(t, batchOpts{firstOffset: 0}, v2Record(0, []byte("k0"), []byte("v0"))),
		controlBatch(t, 1, 0, controlRecordCommit)...,
	)
	r := newControlTestReader(t, data)

	msg, err := readOneMessage(r, 0)
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if string(msg.Value) != "v0" {
		t.Fatalf("first value = %q, want v0", msg.Value)
	}

	msg, err = readOneMessage(r, msg.Offset+1)
	if err == nil {
		t.Fatalf("expected an error, got key=%q value=%q", msg.Key, msg.Value)
	}
	if !errors.Is(err, errShortRead) {
		t.Logf("second readMessage error = %v (any non-nil error ends the batch)", err)
	}
}

// TestControlHeaderPredicate covers the attribute bit itself.
func TestControlHeaderPredicate(t *testing.T) {
	var h messagesHeader
	h.magic = 2
	h.v2.attributes = 0x20
	if !h.control() {
		t.Error("control() = false for a v2 header with the control bit set")
	}

	h.v2.attributes = 0x21 // control + gzip
	if !h.control() {
		t.Error("control() = false when the control bit is set alongside compression")
	}

	h.v2.attributes = 0x00
	if h.control() {
		t.Error("control() = true for an ordinary v2 batch")
	}

	// v0 and v1 have no transactions, so the bit is not meaningful there.
	h.magic = 1
	h.v2.attributes = 0x20
	if h.control() {
		t.Error("control() = true for a v1 header")
	}
}

// TestTransactionalHeaderPredicate covers the transactional attribute bit,
// which is what keeps the aborted-transaction filter away from batches written
// outside a transaction.
func TestTransactionalHeaderPredicate(t *testing.T) {
	var h messagesHeader
	h.magic = 2

	h.v2.attributes = 0x10
	if !h.transactional() {
		t.Error("transactional() = false for a v2 header with the transactional bit set")
	}

	h.v2.attributes = 0x30 // transactional + control, as a marker batch carries
	if !h.transactional() {
		t.Error("transactional() = false when the control bit is set alongside it")
	}

	h.v2.attributes = 0x20 // control only
	if h.transactional() {
		t.Error("transactional() = true for a batch that only set the control bit")
	}

	h.v2.attributes = 0x00
	if h.transactional() {
		t.Error("transactional() = true for an ordinary v2 batch")
	}

	h.magic = 1
	h.v2.attributes = 0x10
	if h.transactional() {
		t.Error("transactional() = true for a v1 header")
	}
}

// TestControlRecordType covers the marker type decoded from a control record's
// key, which decides whether a producer stops being a reason to drop records.
func TestControlRecordType(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		want int16
	}{
		{"abort", []byte{0x00, 0x00, 0x00, 0x00}, controlRecordAbort},
		{"commit", []byte{0x00, 0x00, 0x00, 0x01}, controlRecordCommit},
		{"unrecognized type", []byte{0x00, 0x00, 0x00, 0x09}, 9},
		{"wrong key size", []byte{0x00, 0x00}, controlRecordUnknown},
		{"nil key", nil, controlRecordUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := controlRecordType(test.key); got != test.want {
				t.Errorf("controlRecordType(%x) = %d, want %d", test.key, got, test.want)
			}
		})
	}

	if got := controlRecordTypeName(controlRecordAbort); got != "ABORT" {
		t.Errorf("name of an abort marker = %q, want ABORT", got)
	}
	if got := controlRecordTypeName(controlRecordCommit); got != "COMMIT" {
		t.Errorf("name of a commit marker = %q, want COMMIT", got)
	}
	if got := controlRecordTypeName(controlRecordUnknown); got != "unknown" {
		t.Errorf("name of an unrecognized marker = %q, want unknown", got)
	}
}

// TestReadMessageSkipsAbortedTransaction covers the case issue #1332 reports: a
// ReadCommitted consumer must not see the records of a transaction the broker
// listed as aborted.
func TestReadMessageSkipsAbortedTransaction(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 0, producerID: 7, transactional: true},
		v2Record(0, []byte("k0"), []byte("aborted-0")),
		v2Record(1, []byte("k1"), []byte("aborted-1")))...)
	data = append(data, controlBatch(t, 2, 7, controlRecordAbort)...)
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 3, producerID: 8, transactional: true},
		v2Record(0, []byte("k2"), []byte("committed")))...)
	data = append(data, controlBatch(t, 4, 8, controlRecordCommit)...)

	r := newControlTestReader(t, data, abortedTransaction{ProducerID: 7, FirstOffset: 0})

	if got, want := readAllValues(t, r), []string{"committed"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageKeepsNonTransactionalRecordsWhileFiltering verifies that a
// producer writing outside a transaction is unaffected by another producer's
// abort, even when its batches are interleaved with the aborted ones.
func TestReadMessageKeepsNonTransactionalRecordsWhileFiltering(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 0, producerID: 7, transactional: true},
		v2Record(0, []byte("k0"), []byte("aborted")))...)
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 1, producerID: 9},
		v2Record(0, []byte("k1"), []byte("plain")))...)
	data = append(data, controlBatch(t, 2, 7, controlRecordAbort)...)

	r := newControlTestReader(t, data, abortedTransaction{ProducerID: 7, FirstOffset: 0})

	if got, want := readAllValues(t, r), []string{"plain"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageResumesProducerAfterAbortMarker is the test that pins the
// removal of a producer from the aborted set. A producer that aborts one
// transaction can immediately commit the next one, and those later records must
// be delivered. An implementation that never clears the set passes every other
// test here and fails this one.
func TestReadMessageResumesProducerAfterAbortMarker(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 0, producerID: 7, transactional: true},
		v2Record(0, []byte("k0"), []byte("aborted")))...)
	data = append(data, controlBatch(t, 1, 7, controlRecordAbort)...)
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 2, producerID: 7, transactional: true},
		v2Record(0, []byte("k1"), []byte("committed")))...)
	data = append(data, controlBatch(t, 3, 7, controlRecordCommit)...)

	r := newControlTestReader(t, data, abortedTransaction{ProducerID: 7, FirstOffset: 0})

	if got, want := readAllValues(t, r), []string{"committed"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageStartsFilteringAtTheTransactionFirstOffset verifies that an
// aborted transaction only hides records from its own first offset onwards.
// Earlier records from the same producer belong to a transaction that already
// committed.
func TestReadMessageStartsFilteringAtTheTransactionFirstOffset(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 0, producerID: 7, transactional: true},
		v2Record(0, []byte("k0"), []byte("earlier-commit")))...)
	data = append(data, controlBatch(t, 1, 7, controlRecordCommit)...)
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 2, producerID: 7, transactional: true},
		v2Record(0, []byte("k1"), []byte("aborted")))...)
	data = append(data, controlBatch(t, 3, 7, controlRecordAbort)...)

	r := newControlTestReader(t, data, abortedTransaction{ProducerID: 7, FirstOffset: 2})

	if got, want := readAllValues(t, r), []string{"earlier-commit"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageDeliversTransactionalRecordsWithoutAnAbortList covers
// ReadUncommitted, where the broker sends no aborted transaction list. Nothing
// may be dropped then, markers aside.
func TestReadMessageDeliversTransactionalRecordsWithoutAnAbortList(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 0, producerID: 7, transactional: true},
		v2Record(0, []byte("k0"), []byte("v0")))...)
	data = append(data, controlBatch(t, 1, 7, controlRecordAbort)...)

	r := newControlTestReader(t, data)

	if got, want := readAllValues(t, r), []string{"v0"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageSkipsAbortedCompressedBatch verifies that an aborted batch is
// discarded in its compressed form. skipBatchV2 relies on lengthRemain covering
// exactly the record section, which is what makes that possible; getting it
// wrong desynchronizes the reader and corrupts every batch that follows.
func TestReadMessageSkipsAbortedCompressedBatch(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 0, producerID: 7, transactional: true, codec: new(gzip.Codec)},
		v2Record(0, []byte("k0"), []byte("aborted-0")),
		v2Record(1, []byte("k1"), []byte("aborted-1")))...)
	data = append(data, controlBatch(t, 2, 7, controlRecordAbort)...)
	// A second producer, so that reaching the committed records does not also
	// depend on producer 7 leaving the aborted set.
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 3, producerID: 8, transactional: true, codec: new(gzip.Codec)},
		v2Record(0, []byte("k2"), []byte("committed")))...)
	data = append(data, controlBatch(t, 4, 8, controlRecordCommit)...)

	r := newControlTestReader(t, data, abortedTransaction{ProducerID: 7, FirstOffset: 0})

	if got, want := readAllValues(t, r), []string{"committed"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageSortsAbortedTransactions verifies that a list arriving out of
// offset order is still consumed correctly. Brokers send it sorted, but the
// front-to-back consumption depends on it, so the reader sorts rather than
// trusts.
func TestReadMessageSortsAbortedTransactions(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 0, producerID: 7, transactional: true},
		v2Record(0, []byte("k0"), []byte("aborted-7")))...)
	data = append(data, controlBatch(t, 1, 7, controlRecordAbort)...)
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 2, producerID: 8, transactional: true},
		v2Record(0, []byte("k1"), []byte("aborted-8")))...)
	data = append(data, controlBatch(t, 3, 8, controlRecordAbort)...)
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 4, producerID: 9, transactional: true},
		v2Record(0, []byte("k2"), []byte("committed")))...)
	data = append(data, controlBatch(t, 5, 9, controlRecordCommit)...)

	r := newControlTestReader(t, data,
		abortedTransaction{ProducerID: 8, FirstOffset: 2},
		abortedTransaction{ProducerID: 7, FirstOffset: 0})

	if got, want := readAllValues(t, r), []string{"committed"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageSkipsEmptyControlBatchBetweenData covers a marker batch the
// log cleaner emptied. The cleaner can remove every record a batch held and
// retain the batch itself to preserve the producer's state, so a compacted
// transactional topic serves batches whose record count is zero. Such a batch
// is consumed entirely by its header read, which breaks the assumption that
// the header just inspected belongs to the record about to be read: a control
// check that then consumes "its" record is really discarding the first record
// of the batch that follows.
func TestReadMessageSkipsEmptyControlBatchBetweenData(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t, batchOpts{firstOffset: 0}, v2Record(0, []byte("k0"), []byte("v0")))...)
	data = append(data, emptyBatch(t, batchOpts{firstOffset: 1, producerID: 7, control: true, transactional: true}, 0)...)
	data = append(data, v2Batch(t, batchOpts{firstOffset: 2}, v2Record(0, []byte("k1"), []byte("v1")))...)

	r := newControlTestReader(t, data)

	if got, want := readAllValues(t, r), []string{"v0", "v1"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// TestReadMessageHidesMarkerAfterEmptyBatch is the same header-ownership bug
// from the other side: an emptied data batch ahead of a marker means the
// control check runs against the empty batch's header, and the marker behind
// it is delivered as a message.
func TestReadMessageHidesMarkerAfterEmptyBatch(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t, batchOpts{firstOffset: 0}, v2Record(0, []byte("k0"), []byte("v0")))...)
	data = append(data, emptyBatch(t, batchOpts{firstOffset: 1, producerID: 7, transactional: true}, 0)...)
	data = append(data, controlBatch(t, 2, 7, controlRecordCommit)...)
	data = append(data, v2Batch(t, batchOpts{firstOffset: 3}, v2Record(0, []byte("k1"), []byte("v1")))...)

	r := newControlTestReader(t, data)

	if got, want := readAllValues(t, r), []string{"v0", "v1"}; !equalStrings(got, want) {
		t.Errorf("values = %q, want %q", got, want)
	}
}

// newTestBatch wraps a response body in a Batch without a connection, which is
// enough to exercise the offset bookkeeping. The deadline is in the future so
// that exhausting the response reports io.EOF rather than a timeout.
func newTestBatch(t *testing.T, offset int64, data []byte, aborted ...abortedTransaction) *Batch {
	t.Helper()
	return &Batch{
		msgs:     newControlTestReader(t, data, aborted...),
		offset:   offset,
		deadline: time.Now().Add(time.Minute),
	}
}

// drainBatch reads the batch to exhaustion and returns the values it produced
// along with the offset the next fetch would resume from.
func drainBatch(t *testing.T, batch *Batch) (values []string, next int64) {
	t.Helper()
	for {
		msg, err := batch.ReadMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("ReadMessage after %d messages: %v", len(values), err)
			}
			return values, batch.Offset()
		}
		values = append(values, string(msg.Value))
	}
}

// TestBatchOffsetAdvancesPastTrailingControlBatch pins the offset bookkeeping
// for the layout a transactional producer actually writes: a commit marker
// closes the response. The marker is hidden from the caller, so nothing else
// records that its offset was consumed, and a reader that resumes from the
// marker fetches the same batch forever.
func TestBatchOffsetAdvancesPastTrailingControlBatch(t *testing.T) {
	data := append(
		v2Batch(t, batchOpts{firstOffset: 0, producerID: 7, transactional: true},
			v2Record(0, []byte("k0"), []byte("v0"))),
		controlBatch(t, 1, 7, controlRecordCommit)...,
	)

	values, next := drainBatch(t, newTestBatch(t, 0, data))

	if want := []string{"v0"}; !equalStrings(values, want) {
		t.Errorf("values = %q, want %q", values, want)
	}
	if next != 2 {
		t.Errorf("next offset = %d, want 2 (past the marker at offset 1)", next)
	}
}

// TestBatchOffsetAdvancesPastAllControlResponse is the second half of the same
// bug, and the more damaging one. Once the reader resumes at the marker, the
// next response holds nothing but that marker: no message is read, lastOffset
// keeps its zero value, and the offset fixup used to reset the partition to 1.
func TestBatchOffsetAdvancesPastAllControlResponse(t *testing.T) {
	data := append(
		controlBatch(t, 100, 7, controlRecordCommit),
		controlBatch(t, 101, 8, controlRecordCommit)...,
	)

	values, next := drainBatch(t, newTestBatch(t, 100, data))

	if len(values) != 0 {
		t.Errorf("values = %q, want none", values)
	}
	if next != 102 {
		t.Errorf("next offset = %d, want 102; a value of 1 is the partition rewind this guards", next)
	}
}

// TestBatchOffsetAdvancesPastAbortedTail covers the same accounting for a
// response whose tail is an aborted transaction rather than a lone marker.
func TestBatchOffsetAdvancesPastAbortedTail(t *testing.T) {
	var data []byte
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 10, producerID: 7, transactional: true},
		v2Record(0, []byte("k0"), []byte("v0")))...)
	data = append(data, controlBatch(t, 11, 7, controlRecordCommit)...)
	data = append(data, v2Batch(t,
		batchOpts{firstOffset: 12, producerID: 8, transactional: true},
		v2Record(0, []byte("k1"), []byte("aborted-0")),
		v2Record(1, []byte("k2"), []byte("aborted-1")))...)
	data = append(data, controlBatch(t, 14, 8, controlRecordAbort)...)

	values, next := drainBatch(t, newTestBatch(t, 10, data,
		abortedTransaction{ProducerID: 8, FirstOffset: 12}))

	if want := []string{"v0"}; !equalStrings(values, want) {
		t.Errorf("values = %q, want %q", values, want)
	}
	if next != 15 {
		t.Errorf("next offset = %d, want 15 (past the abort marker at offset 14)", next)
	}
}

// TestBatchOffsetAdvancesPastTrailingEmptyControlBatch pins the offset
// bookkeeping for an emptied marker closing the response. Its offsets are
// consumed by the header read alone, and a reader that fails to account for
// them resumes at the empty batch and fetches it forever.
func TestBatchOffsetAdvancesPastTrailingEmptyControlBatch(t *testing.T) {
	data := append(
		v2Batch(t, batchOpts{firstOffset: 0, producerID: 7, transactional: true},
			v2Record(0, []byte("k0"), []byte("v0"))),
		emptyBatch(t, batchOpts{firstOffset: 1, producerID: 7, control: true, transactional: true}, 0)...,
	)

	values, next := drainBatch(t, newTestBatch(t, 0, data))

	if want := []string{"v0"}; !equalStrings(values, want) {
		t.Errorf("values = %q, want %q", values, want)
	}
	if next != 2 {
		t.Errorf("next offset = %d, want 2 (past the empty batch at offset 1)", next)
	}
}

// TestBatchOffsetAdvancesPastAllEmptyResponse is the refetch that follows once
// a reader resumes at an emptied batch: the response holds nothing but the
// empty batch, and the offset still has to move past it.
func TestBatchOffsetAdvancesPastAllEmptyResponse(t *testing.T) {
	data := emptyBatch(t, batchOpts{firstOffset: 5, producerID: 7, control: true, transactional: true}, 0)

	values, next := drainBatch(t, newTestBatch(t, 5, data))

	if len(values) != 0 {
		t.Errorf("values = %q, want none", values)
	}
	if next != 6 {
		t.Errorf("next offset = %d, want 6 (past the empty batch at offset 5)", next)
	}
}

// TestBatchOffsetNeverRewinds asserts the invariant the fixup relies on: a
// response that yields no message at all leaves the caller's position alone
// rather than moving it backwards.
func TestBatchOffsetNeverRewinds(t *testing.T) {
	batch := newTestBatch(t, 500, controlBatch(t, 400, 7, controlRecordCommit))

	if _, next := drainBatch(t, batch); next < 500 {
		t.Errorf("next offset = %d, want no less than the 500 the batch started at", next)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
