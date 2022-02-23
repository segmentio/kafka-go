package protocol

import (
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

func (rs *RecordSet) readFromVersion2(decoder *decoder) (int64, error) {
	const baseLength = 12
	baseOffset := decoder.readInt64()
	batchLength := decoder.readInt32()

	if int(batchLength) > decoder.remain || decoder.err != nil {
		n := int64(decoder.remain)
		decoder.discardAll()
		return baseLength + n, nil
	}

	leftover := decoder.remain - int(batchLength)
	buffer := newPageBuffer()
	defer func() {
		decoder.remain = leftover
		if buffer != nil {
			buffer.unref()
		}
	}()

	decoder.remain = int(batchLength)
	n, err := buffer.ReadFrom(decoder)
	if err != nil {
		return baseLength + n, err
	}

	decoder.Reset(buffer, int(n))
	partitionLeaderEpoch := decoder.readInt32()
	magicByte := decoder.readInt8()
	crc := decoder.readInt32()
	checksum := buffer.crc32()

	if checksum != uint32(crc) {
		return baseLength + n, fmt.Errorf("crc32 checksum mismatch (computed=%d found=%d)", checksum, uint32(crc))
	}

	attributes := Attributes(decoder.readInt16())
	lastOffsetDelta := decoder.readInt32()
	firstTimestamp := decoder.readInt64()
	maxTimestamp := decoder.readInt64()
	producerID := decoder.readInt64()
	producerEpoch := decoder.readInt16()
	baseSequence := decoder.readInt32()
	numRecords := decoder.readInt32()

	*rs = RecordSet{
		Version:    magicByte,
		Attributes: attributes,
	}

	recordBatch := &RecordBatch{
		Attributes:           attributes,
		PartitionLeaderEpoch: partitionLeaderEpoch,
		BaseOffset:           baseOffset,
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       MakeTime(firstTimestamp),
		MaxTimestamp:         MakeTime(maxTimestamp),
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
		NumRecords:           numRecords,
		Records: &compressedRecordReader{
			buffer:         buffer,
			attributes:     attributes,
			baseOffset:     baseOffset,
			firstTimestamp: firstTimestamp,
			numRecords:     numRecords,
		},
	}

	if attributes.Control() {
		rs.Records = (*ControlBatch)(recordBatch)
	} else {
		rs.Records = recordBatch
	}

	buffer = nil
	return baseLength + n, nil
}

func readRecords(buffer *pageBuffer, decoder *decoder, attributes Attributes, baseOffset, firstTimestamp int64, numRecords int32) (*optimizedRecordReader, error) {
	bufferOffset := int64(buffer.cursor)

	if compression := attributes.Compression(); compression != 0 {
		reader := io.Reader(buffer)
		buffer = newPageBuffer()
		bufferOffset = 0
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
		keyOffset := bufferOffset + int64(recordsLength-decoder.remain)
		if keyLength > 0 {
			decoder.discard(int(keyLength))
		}

		valueLength := decoder.readVarInt()
		valueOffset := bufferOffset + int64(recordsLength-decoder.remain)
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
	attributes := rs.Attributes
	records := rs.Records
	baseOffset := int64(0)
	lastOffsetDelta := int32(0)
	firstTimestamp := int64(0)
	maxTimestamp := int64(0)
	numRecords := int32(0)

	batch, _ := records.(*RecordBatch)
	if batch != nil {
		baseOffset = batch.BaseOffset
		if batch.Attributes != attributes {
			// The application requested a different set of attributes,
			// we can't optimize writing the batch because we may have
			// to change the compression codec or other properties.
			batch = nil
		}
	}

	e := &encoder{writer: buffer}
	e.writeInt64(baseOffset)        // base offset                         |  0 +8
	e.writeInt32(0)                 // placeholder for record batch length |  8 +4
	e.writeInt32(-1)                // partition leader epoch              | 12 +3
	e.writeInt8(2)                  // magic byte                          | 16 +1
	e.writeInt32(0)                 // placeholder for crc32 checksum      | 17 +4
	e.writeInt16(int16(attributes)) // attributes                          | 21 +2
	e.writeInt32(0)                 // placeholder for lastOffsetDelta     | 23 +4
	e.writeInt64(0)                 // placeholder for firstTimestamp      | 27 +8
	e.writeInt64(0)                 // placeholder for maxTimestamp        | 35 +8
	e.writeInt64(-1)                // producer id                         | 43 +8
	e.writeInt16(-1)                // producer epoch                      | 51 +2
	e.writeInt32(-1)                // base sequence                       | 53 +4
	e.writeInt32(0)                 // placeholder for numRecords          | 57 +4

	var err error
	if batch != nil {
		// If the record batch contains pre-encoded records, we avoid decoding
		// them and just write the bytes directly to the output buffer.
		if r, ok := batch.Records.(io.Reader); ok {
			lastOffsetDelta = batch.LastOffsetDelta
			firstTimestamp = Timestamp(batch.FirstTimestamp)
			maxTimestamp = Timestamp(batch.MaxTimestamp)
			numRecords = batch.NumRecords
			records = nil
			_, err = io.Copy(e, r)
		}
	}

	if records != nil {
		// No optimizations could be applied, we have to read each record and
		// write them to the output buffer while generating the metadata.
		var currentTimestamp = Timestamp(time.Now())
		var compressor io.WriteCloser

		if compression := rs.Attributes.Compression(); compression != 0 {
			if codec := compression.Codec(); codec != nil {
				compressor = codec.NewWriter(buffer)
				e.writer = compressor
			}
		}

		err = forEachRecord(records, func(i int, r *Record) error {
			t := Timestamp(r.Time)
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

		if compressor != nil {
			if err := compressor.Close(); err != nil {
				return err
			}
		}
	}

	if err != nil {
		return err
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
