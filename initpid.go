package kafka

import "bufio"

type initProducerIDRequestV0 struct {
	// The transactional id or null if the producer is not transactional
	TransactionalId string

	// The time in ms to wait for before aborting idle transactions sent by
	// this producer.
	TransactionTimeoutMs int32
}

func (t initProducerIDRequestV0) size() int32 {
	return sizeof(t.TransactionalId) +
		sizeof(t.TransactionTimeoutMs)
}

func (t initProducerIDRequestV0) writeTo(w *bufio.Writer) {
	if len(t.TransactionalId) == 0 {
		writeNullableString(w, nil)
	} else {
		writeNullableString(w, &t.TransactionalId)
	}
	writeInt32(w, t.TransactionTimeoutMs)
}

type initProducerIDResponseV0 struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ProducerID     int64
	ProducerEpoch  int16
}

func (t initProducerIDResponseV0) size() int32 {
	return sizeof(t.ThrottleTimeMs) +
		sizeof(t.ErrorCode) +
		sizeof(t.ProducerID) +
		sizeof(t.ProducerEpoch)
}

func (t initProducerIDResponseV0) writeTo(w *bufio.Writer) {
	writeInt32(w, t.ThrottleTimeMs)
	writeInt16(w, t.ErrorCode)
	writeInt64(w, t.ProducerID)
	writeInt16(w, t.ProducerEpoch)
}

func (t *initProducerIDResponseV0) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMs); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readInt64(r, remain, &t.ProducerID); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ProducerEpoch); err != nil {
		return
	}

	return
}
