package protocol

import (
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

func (rs *RecordSet) readFromVersion2(d *decoder) error {
	baseOffset := d.readInt64()
	batchLength := d.readInt32()

	if int(batchLength) > d.remain || d.err != nil {
		d.discardAll()
		return nil
	}

	decoder := &decoder{
		reader: d,
		remain: int(batchLength),
	}

	partitionLeaderEpoch := decoder.readInt32()
	magicByte := decoder.readInt8()
	crc := decoder.readInt32()
	decoder.setCRC(crc32.MakeTable(crc32.Castagnoli))

	attributes := Attributes(decoder.readInt16())
	lastOffsetDelta := decoder.readInt32()
	firstTimestamp := decoder.readInt64()
	maxTimestamp := decoder.readInt64()
	producerID := decoder.readInt64()
	producerEpoch := decoder.readInt16()
	baseSequence := decoder.readInt32()
	numRecords := decoder.readInt32()

	buffer := newPageBuffer()
	defer func() {
		if buffer != nil {
			buffer.unref()
		}
	}()

	_, err := buffer.ReadFrom(decoder)
	if err != nil {
		return err
	}
	if decoder.crc32 != uint32(crc) {
		return fmt.Errorf("crc32 checksum mismatch (computed=%d found=%d)", decoder.crc32, uint32(crc))
	}
	decoder.Reset(nil, 0)

	*rs = RecordSet{
		Version:    magicByte,
		Attributes: attributes,
	}

	recordBatch := &RecordBatch{
		Attributes:           attributes,
		PartitionLeaderEpoch: partitionLeaderEpoch,
		BaseOffset:           baseOffset,
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       makeTime(firstTimestamp),
		MaxTimestamp:         makeTime(maxTimestamp),
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
	}

	if attributes.Control() {
		recordBatch.Records, err = readRecords(buffer, decoder, attributes, baseOffset, firstTimestamp, numRecords)
		if err != nil {
			return err
		}
		rs.Records = (*ControlBatch)(recordBatch)
	} else {
		rs.Records = recordBatch
		recordBatch.Records = &compressedRecordReader{
			buffer:         buffer,
			decoder:        decoder,
			attributes:     attributes,
			baseOffset:     baseOffset,
			firstTimestamp: firstTimestamp,
			numRecords:     numRecords,
		}
		buffer = nil
	}

	return nil
}

func readRecords(buffer *pageBuffer, decoder *decoder, attributes Attributes, baseOffset, firstTimestamp int64, numRecords int32) (*optimizedRecordReader, error) {
	if compression := attributes.Compression(); compression != 0 {
		reader := io.Reader(buffer)
		buffer = newPageBuffer()
		defer buffer.unref()

		codec := compression.Codec()
		if codec == nil {
			return nil, fmt.Errorf("unsupported compression codec (%d)", compression)
		}

		decompressor := codec.NewReader(reader)
		_, err := buffer.ReadFrom(decompressor)
		decompressor.Close()
		if err != nil {
			return nil, err
		}
	}

	recordsLength := buffer.Len()
	decoder.Reset(buffer, recordsLength)

	records := make([]optimizedRecord, numRecords)
	// These are two lazy allocators that will be used to optimize allocation of
	// page references for keys and values.
	//
	// By default, no memory is allocated and on first use, numRecords page refs
	// are allocated in a contiguous memory space, and the allocators return
	// pointers into those arrays for each page ref that get requested.
	//
	// The reasoning is that kafka partitions typically have records of a single
	// form, which either have no keys, no values, or both keys and values.
	// Using lazy allocators adapts nicely to these patterns to only allocate
	// the memory that is needed by the program, while still reducing the number
	// of malloc calls made by the program.
	//
	// Using a single allocator for both keys and values keeps related values
	// close by in memory, making access to the records more friendly to CPU
	// caches.
	alloc := pageRefAllocator{size: int(numRecords)}
	// Following the same reasoning that kafka partitions will typically have
	// records with repeating formats, we expect to either find records with
	// no headers, or records which always contain headers.
	//
	// To reduce the memory footprint when records have no headers, the Header
	// slices are lazily allocated in a separate array.
	headers := ([][]Header)(nil)

	for i := range records {
		r := &records[i]
		_ = decoder.readVarInt() // record length (unused)
		_ = decoder.readInt8()   // record attributes (unused)
		timestampDelta := decoder.readVarInt()
		offsetDelta := decoder.readVarInt()

		r.offset = baseOffset + offsetDelta
		r.timestamp = firstTimestamp + timestampDelta

		keyLength := decoder.readVarInt()
		keyOffset := int64(recordsLength - decoder.remain)
		if keyLength > 0 {
			decoder.discard(int(keyLength))
		}

		valueLength := decoder.readVarInt()
		valueOffset := int64(recordsLength - decoder.remain)
		if valueLength > 0 {
			decoder.discard(int(valueLength))
		}

		if numHeaders := decoder.readVarInt(); numHeaders > 0 {
			if headers == nil {
				headers = make([][]Header, numRecords)
			}

			h := make([]Header, numHeaders)

			for i := range h {
				h[i] = Header{
					Key:   decoder.readVarString(),
					Value: decoder.readVarBytes(),
				}
			}

			headers[i] = h
		}

		if decoder.err != nil {
			records = records[:i]
			break
		}

		if keyLength >= 0 {
			r.keyRef = alloc.newPageRef()
			buffer.refTo(r.keyRef, keyOffset, keyOffset+keyLength)
		}

		if valueLength >= 0 {
			r.valueRef = alloc.newPageRef()
			buffer.refTo(r.valueRef, valueOffset, valueOffset+valueLength)
		}
	}

	// Note: it's unclear whether kafka 0.11+ still truncates the responses,
	// all attempts I made at constructing a test to trigger a truncation have
	// failed. I kept this code here as a safeguard but it may never execute.
	if decoder.err != nil && len(records) == 0 {
		return nil, decoder.err
	}

	return &optimizedRecordReader{records: records, headers: headers}, nil
}

func (rs *RecordSet) writeToVersion2(buffer *pageBuffer, bufferOffset int64) error {
	records := rs.Records
	numRecords := int32(0)

	e := &encoder{writer: buffer}
	e.writeInt64(0)                    // base offset                         |  0 +8
	e.writeInt32(0)                    // placeholder for record batch length |  8 +4
	e.writeInt32(-1)                   // partition leader epoch              | 12 +3
	e.writeInt8(2)                     // magic byte                          | 16 +1
	e.writeInt32(0)                    // placeholder for crc32 checksum      | 17 +4
	e.writeInt16(int16(rs.Attributes)) // attributes                          | 21 +2
	e.writeInt32(0)                    // placeholder for lastOffsetDelta     | 23 +4
	e.writeInt64(0)                    // placeholder for firstTimestamp      | 27 +8
	e.writeInt64(0)                    // placeholder for maxTimestamp        | 35 +8
	e.writeInt64(-1)                   // producer id                         | 43 +8
	e.writeInt16(-1)                   // producer epoch                      | 51 +2
	e.writeInt32(-1)                   // base sequence                       | 53 +4
	e.writeInt32(0)                    // placeholder for numRecords          | 57 +4

	var compressor io.WriteCloser
	if compression := rs.Attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			compressor = codec.NewWriter(buffer)
			e.writer = compressor
		}
	}

	currentTimestamp := timestamp(time.Now())
	lastOffsetDelta := int32(0)
	firstTimestamp := int64(0)
	maxTimestamp := int64(0)

	err := forEachRecord(records, func(i int, r *Record) error {
		t := timestamp(r.Time)
		if t == 0 {
			t = currentTimestamp
		}
		if i == 0 {
			firstTimestamp = t
		}
		if t > maxTimestamp {
			maxTimestamp = t
		}

		timestampDelta := t - firstTimestamp
		offsetDelta := int64(i)
		lastOffsetDelta = int32(offsetDelta)

		length := 1 + // attributes
			sizeOfVarInt(timestampDelta) +
			sizeOfVarInt(offsetDelta) +
			sizeOfVarNullBytesIface(r.Key) +
			sizeOfVarNullBytesIface(r.Value) +
			sizeOfVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			length += sizeOfVarString(h.Key) + sizeOfVarNullBytes(h.Value)
		}

		e.writeVarInt(int64(length))
		e.writeInt8(0) // record attributes (unused)
		e.writeVarInt(timestampDelta)
		e.writeVarInt(offsetDelta)

		if err := e.writeVarNullBytesFrom(r.Key); err != nil {
			return err
		}

		if err := e.writeVarNullBytesFrom(r.Value); err != nil {
			return err
		}

		e.writeVarInt(int64(len(r.Headers)))

		for _, h := range r.Headers {
			e.writeVarString(h.Key)
			e.writeVarNullBytes(h.Value)
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
		return ErrNoRecord
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
