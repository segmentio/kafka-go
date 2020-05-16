package protocol

import (
	"hash/crc32"
	"io"
)

func (rs *RecordSet) readFromVersion2(d *decoder) error {
	baseOffset := d.readInt64()
	batchLength := d.readInt32()

	if int(batchLength) > d.remain || d.err != nil {
		d.discardAll()
		return nil
	}

	dec := &decoder{
		reader: d,
		remain: int(batchLength),
	}

	partitionLeaderEpoch := dec.readInt32()
	magicByte := dec.readInt8()
	crc := dec.readInt32()

	dec.setCRC(crc32.MakeTable(crc32.Castagnoli))

	attributes := dec.readInt16()
	lastOffsetDelta := dec.readInt32()
	firstTimestamp := dec.readInt64()
	maxTimestamp := dec.readInt64()
	producerID := dec.readInt64()
	producerEpoch := dec.readInt16()
	baseSequence := dec.readInt32()
	numRecords := dec.readInt32()
	reader := io.Reader(dec)

	// unused
	_ = lastOffsetDelta
	_ = maxTimestamp

	if compression := Attributes(attributes).Compression(); compression != 0 {
		codec := compression.Codec()
		if codec == nil {
			return errorf("unsupported compression codec (%d)", compression)
		}
		decompressor := codec.NewReader(reader)
		defer decompressor.Close()
		reader = decompressor
	}

	buffer := newPageBuffer()
	defer buffer.unref()

	_, err := buffer.ReadFrom(reader)
	if err != nil {
		return err
	}
	if dec.crc32 != uint32(crc) {
		return errorf("crc32 checksum mismatch (computed=%d found=%d)", dec.crc32, uint32(crc))
	}

	recordsLength := buffer.Len()
	dec.reader = buffer
	dec.remain = recordsLength
	records := make([]optimizedRecord, numRecords)

	for i := range records {
		r := &records[i]
		_ = dec.readVarInt() // record length (unused)
		_ = dec.readInt8()   // record attributes (unused)
		timestampDelta := dec.readVarInt()
		offsetDelta := dec.readVarInt()

		r.offset = baseOffset + offsetDelta
		r.timestamp = firstTimestamp + timestampDelta

		keyLength := dec.readVarInt()
		keyOffset := int64(recordsLength - dec.remain)
		if keyLength > 0 {
			dec.discard(int(keyLength))
		}

		valueLength := dec.readVarInt()
		valueOffset := int64(recordsLength - dec.remain)
		if valueLength > 0 {
			dec.discard(int(valueLength))
		}

		if numHeaders := dec.readVarInt(); numHeaders > 0 {
			r.headers = make([]Header, numHeaders)

			for i := range r.headers {
				r.headers[i] = Header{
					Key:   dec.readCompactString(),
					Value: dec.readCompactBytes(),
				}
			}
		}

		if dec.err != nil {
			records = records[:i]
			break
		}

		if keyLength >= 0 {
			buffer.refTo(&r.keyRef, keyOffset, keyOffset+keyLength)
		}

		if valueLength >= 0 {
			buffer.refTo(&r.valueRef, valueOffset, valueOffset+valueLength)
		}
	}

	// Note: it's unclear whether kafka 0.11+ still truncates the responses,
	// all attempts I made at constructing a test to trigger a truncation have
	// failed. I kept this code here as a safeguard but it may never execute.
	if dec.err != nil && len(records) == 0 {
		return dec.err
	}

	*rs = RecordSet{
		Version:              magicByte,
		Attributes:           Attributes(attributes),
		PartitionLeaderEpoch: partitionLeaderEpoch,
		BaseOffset:           baseOffset,
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
		Records:              &optimizedRecordBatch{records: records},
	}

	return nil
}

func (rs *RecordSet) writeToVersion2(buffer *pageBuffer, bufferOffset int64) error {
	records := rs.Records
	baseOffset := rs.BaseOffset
	numRecords := int32(0)

	e := &encoder{writer: buffer}
	e.writeInt64(baseOffset)              //                                     |  0 +8
	e.writeInt32(0)                       // placeholder for record batch length |  8 +4
	e.writeInt32(rs.PartitionLeaderEpoch) //                                     | 12 +3
	e.writeInt8(2)                        // magic byte                          | 16 +1
	e.writeInt32(0)                       // placeholder for crc32 checksum      | 17 +4
	e.writeInt16(int16(rs.Attributes))    //                                     | 21 +2
	e.writeInt32(0)                       // placeholder for lastOffsetDelta     | 23 +4
	e.writeInt64(0)                       // placeholder for firstTimestamp      | 27 +8
	e.writeInt64(0)                       // placeholder for maxTimestamp        | 35 +8
	e.writeInt64(rs.ProducerID)           //                                     | 43 +8
	e.writeInt16(rs.ProducerEpoch)        //                                     | 51 +2
	e.writeInt32(rs.BaseSequence)         //                                     | 53 +4
	e.writeInt32(0)                       // placeholder for numRecords          | 57 +4

	var compressor io.WriteCloser
	if compression := rs.Attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			compressor = codec.NewWriter(buffer)
			e.writer = compressor
		}
	}

	lastOffsetDelta := int32(0)
	firstTimestamp := int64(0)
	maxTimestamp := int64(0)

	err := forEachRecord(records, func(i int, r *Record) error {
		maxTimestamp = timestamp(r.Time)
		if i == 0 {
			firstTimestamp = maxTimestamp
		}

		timestampDelta := maxTimestamp - firstTimestamp
		offsetDelta := r.Offset - baseOffset
		lastOffsetDelta = int32(offsetDelta)

		length := 1 + // attributes
			sizeOfVarInt(timestampDelta) +
			sizeOfVarInt(offsetDelta) +
			sizeOfVarBytes(r.Key) +
			sizeOfVarBytes(r.Value) +
			sizeOfVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			length += sizeOfCompactString(h.Key) + sizeOfCompactNullBytes(h.Value)
		}

		e.writeVarInt(int64(length))
		e.writeInt8(0) // record attributes (unused)
		e.writeVarInt(timestampDelta)
		e.writeVarInt(offsetDelta)

		if err := e.writeCompactNullBytesFrom(r.Key); err != nil {
			return err
		}

		if err := e.writeCompactNullBytesFrom(r.Value); err != nil {
			return err
		}

		e.writeVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			e.writeCompactString(h.Key)
			e.writeCompactNullBytes(h.Value)
		}

		numRecords++
		return nil
	})

	if err != nil {
		return err
	}

	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return err
		}
	}

	if numRecords == 0 {
		return ErrNoRecords
	}

	b2 := packUint32(uint32(lastOffsetDelta))
	b3 := packUint64(uint64(firstTimestamp))
	b4 := packUint64(uint64(maxTimestamp))
	b5 := packUint32(uint32(numRecords))

	buffer.WriteAt(b2[:], bufferOffset+23)
	buffer.WriteAt(b3[:], bufferOffset+27)
	buffer.WriteAt(b4[:], bufferOffset+35)
	buffer.WriteAt(b5[:], bufferOffset+57)

	totalLength := buffer.Size() - bufferOffset
	batchLength := totalLength - 12

	checksum := uint32(0)
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	buffer.pages.scan(bufferOffset+21, bufferOffset+totalLength, func(chunk []byte) bool {
		checksum = crc32.Update(checksum, crcTable, chunk)
		return true
	})

	b0 := packUint32(uint32(batchLength))
	b1 := packUint32(checksum)

	buffer.WriteAt(b0[:], bufferOffset+8)
	buffer.WriteAt(b1[:], bufferOffset+17)
	return nil
}
