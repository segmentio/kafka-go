package kafka

import (
	"bytes"
)

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

func compressRecordBatch(codec CompressionCodec, msgs ...Message) (compressed *bytes.Buffer, attributes int16, size int32, err error) {
	compressed = acquireBuffer()
	compressor := codec.NewWriter(compressed)
	wb := &writeBuffer{w: compressor}

	for i, msg := range msgs {
		wb.writeRecord(0, msgs[0].Time, int64(i), msg)
	}

	if err = compressor.Close(); err != nil {
		releaseBuffer(compressed)
		return
	}

	attributes = int16(codec.Code())
	size = recordBatchHeaderSize + int32(compressed.Len())
	return
}

type recordBatch struct {
	// required input parameters
	codec      CompressionCodec
	attributes int16
	msgs       []Message

	// parameters calculated during init
	compressed *bytes.Buffer
	size       int32
}

func (r *recordBatch) init() (err error) {
	if r.codec == nil {
		r.size = recordBatchSize(r.msgs...)
	} else {
		r.compressed, r.attributes, r.size, err = compressRecordBatch(r.codec, r.msgs...)
	}
	return
}

func (r *recordBatch) writeTo(wb *writeBuffer) {
	baseTime := r.msgs[0].Time
	lastTime := r.msgs[len(r.msgs)-1].Time
	if r.compressed != nil {
		wb.writeRecordBatch(r.attributes, r.size, len(r.msgs), baseTime, lastTime, func(wb *writeBuffer) {
			wb.Write(r.compressed.Bytes())
		})
		releaseBuffer(r.compressed)
	} else {
		wb.writeRecordBatch(r.attributes, r.size, len(r.msgs), baseTime, lastTime, func(wb *writeBuffer) {
			for i, msg := range r.msgs {
				wb.writeRecord(0, r.msgs[0].Time, int64(i), msg)
			}
		})
	}
}
