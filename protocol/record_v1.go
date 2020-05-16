package protocol

import (
	"errors"
	"hash/crc32"
	"io"
	"math"

	"github.com/segmentio/kafka-go/compress"
)

func (rs *RecordSet) readFromVersion1(d *decoder) error {
	records := make([]Record, 0, 100)
	defer func() {
		for _, r := range records {
			unref(r.Key)
			unref(r.Value)
		}
	}()

	buffer := newPageBuffer()
	defer buffer.unref()

	baseAttributes := int8(0)
	md := decoder{reader: d}

	for !d.done() {
		md.remain = 12
		offset := md.readInt64()
		md.remain = int(md.readInt32())

		if md.remain == 0 {
			break
		}

		crc := uint32(md.readInt32())
		md.setCRC(crc32.IEEETable)
		magicByte := md.readInt8()
		attributes := md.readInt8()
		timestamp := int64(0)

		if magicByte != 0 {
			timestamp = md.readInt64()
		}

		baseAttributes |= attributes
		var compression = Attributes(attributes).Compression()
		var codec compress.Codec
		if compression != 0 {
			if codec = compression.Codec(); codec == nil {
				return errorf("unsupported compression codec: %d", compression)
			}
		}

		var k Bytes
		var v Bytes

		keyOffset := buffer.Size()
		keyLength := int(md.readInt32())
		hasKey := keyLength >= 0

		if hasKey {
			md.writeTo(buffer, keyLength)
			k = buffer.ref(keyOffset, buffer.Size())
		}

		valueOffset := buffer.Size()
		valueLength := int(md.readInt32())
		hasValue := valueLength >= 0

		if hasValue {
			if codec == nil {
				md.writeTo(buffer, valueLength)
				v = buffer.ref(valueOffset, buffer.Size())
			} else {
				subset := RecordSet{}
				decompressor := codec.NewReader(&md)

				err := subset.readFromVersion1(&decoder{
					reader: decompressor,
					remain: math.MaxInt32,
				})

				decompressor.Close()

				if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
					return err
				}

				unref(k) // is it valid to have a non-nil key on the root message?
				records = append(records, subset.Records.(*recordBatch).records...)
			}
		}

		// When a compression algorithm was configured on the attributes, there
		// is no actual value in this loop iterator, instead the decompressed
		// records have been loaded by calling readFromVersion1 recursively and
		// already added to the `records` list.
		if compression == 0 && md.err == nil {
			records = append(records, Record{
				Offset: offset,
				Time:   makeTime(timestamp),
				Key:    k,
				Value:  v,
			})
		}

		// When we reach this point, we should already have consumed all the
		// bytes in the message. If that's not the case for any reason, discard
		// whatever is left so we position the stream properly to read the next
		// message.
		if !md.done() {
			md.discardAll()
		}

		// io.ErrUnexpectedEOF could bubble up here if the kafka broker has
		// truncated the response. We can safely handle this error and assume
		// that we have reached the end of the message set.
		if md.err != nil {
			if errors.Is(md.err, io.ErrUnexpectedEOF) {
				break
			}
			return md.err
		}

		if md.crc32 != crc {
			return errorf("crc32 checksum mismatch (computed=%d found=%d)", md.crc32, crc)
		}
	}

	*rs = RecordSet{
		Version:    1,
		Attributes: Attributes(baseAttributes),
		Records: &recordBatch{
			records: records,
		},
	}

	if len(records) != 0 {
		rs.BaseOffset = records[0].Offset
	}

	records = nil
	return nil
}

func (rs *RecordSet) writeToVersion1(buffer *pageBuffer, bufferOffset int64) error {
	attributes := rs.Attributes
	records := rs.Records

	if compression := attributes.Compression(); compression != 0 {
		if codec := compression.Codec(); codec != nil {
			// In the message format version 1, compression is acheived by
			// compressing the value of a message which recursively contains
			// the representation of the compressed message set.
			subset := *rs
			subset.Attributes &= ^7 // erase compression

			if err := subset.writeToVersion1(buffer, bufferOffset); err != nil {
				return err
			}

			compressed := newPageBuffer()
			defer compressed.unref()

			compressor := codec.NewWriter(compressed)
			defer compressor.Close()

			var err error
			buffer.pages.scan(bufferOffset, buffer.Size(), func(b []byte) bool {
				_, err = compressor.Write(b)
				return err != nil
			})
			if err != nil {
				return err
			}
			if err := compressor.Close(); err != nil {
				return err
			}

			buffer.Truncate(int(bufferOffset))

			records = &recordBatch{
				records: []Record{{
					Value: compressed,
				}},
			}
		}
	}

	e := encoder{writer: buffer}

	return forEachRecord(records, func(i int, r *Record) error {
		messageOffset := buffer.Size()
		e.writeInt64(r.Offset)
		e.writeInt32(0) // message size placeholder
		e.writeInt32(0) // crc32 placeholder
		e.setCRC(crc32.IEEETable)
		e.writeInt8(1) // magic byte: version 1
		e.writeInt8(int8(attributes))
		e.writeInt64(timestamp(r.Time))

		if err := e.writeNullBytesFrom(r.Key); err != nil {
			return err
		}

		if err := e.writeNullBytesFrom(r.Value); err != nil {
			return err
		}

		b0 := packUint32(uint32(buffer.Size() - (messageOffset + 12)))
		b1 := packUint32(e.crc32)

		buffer.WriteAt(b0[:], messageOffset+8)
		buffer.WriteAt(b1[:], messageOffset+12)
		e.setCRC(nil)
		return nil
	})
}
