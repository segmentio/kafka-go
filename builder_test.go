package kafka

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go/compress"
)

// This file defines builders to assist in creating kafka payloads for unit testing.

// fetchResponseBuilder builds v10 fetch responses. The version of the v10 fetch
// responses are not as important as the message sets contained within, as this
// type is ultimately used to unit test the message set reader that consumes the
// rest of the response once the header has been parsed.
type fetchResponseBuilder struct {
	header   fetchResponseHeader
	msgSets  []messageSetBuilder
	rendered []byte
}

type fetchResponseHeader struct {
	throttle            int32
	errorCode           int16
	sessionID           int32
	topic               string
	partition           int32
	partitionErrorCode  int16
	highWatermarkOffset int64
	lastStableOffset    int64
	logStartOffset      int64
}

func (b *fetchResponseBuilder) messages() (res []Message) {
	for _, set := range b.msgSets {
		res = append(res, set.messages()...)
	}
	return
}

func (b *fetchResponseBuilder) bytes() []byte {
	if b.rendered == nil {
		b.rendered = newWB().call(func(wb *kafkaWriteBuffer) {
			wb.writeInt32(b.header.throttle)
			wb.writeInt16(b.header.errorCode)
			wb.writeInt32(b.header.sessionID)
			wb.writeInt32(1) // num topics
			wb.writeString(b.header.topic)
			wb.writeInt32(1) // how many partitions
			wb.writeInt32(b.header.partition)
			wb.writeInt16(b.header.partitionErrorCode)
			wb.writeInt64(b.header.highWatermarkOffset)
			wb.writeInt64(b.header.lastStableOffset)
			wb.writeInt64(b.header.logStartOffset)
			wb.writeInt32(-1) // num aborted tx
			wb.writeBytes(newWB().call(func(wb *kafkaWriteBuffer) {
				for _, msgSet := range b.msgSets {
					wb.Write(msgSet.bytes())
				}
			}))
		})
	}
	return b.rendered
}

func (b *fetchResponseBuilder) Len() int {
	return len(b.bytes())
}

type messageSetBuilder interface {
	bytes() []byte
	messages() []Message
}

type v0MessageSetBuilder struct {
	msgs  []Message
	codec CompressionCodec
}

func (f v0MessageSetBuilder) messages() []Message {
	return f.msgs
}

func (f v0MessageSetBuilder) bytes() []byte {
	bs := newWB().call(func(wb *kafkaWriteBuffer) {
		for _, msg := range f.msgs {
			bs := newWB().call(func(wb *kafkaWriteBuffer) {
				wb.writeInt64(msg.Offset) // offset
				wb.writeBytes(newWB().call(func(wb *kafkaWriteBuffer) {
					wb.writeInt32(-1) // crc, unused
					wb.writeInt8(0)   // magic
					wb.writeInt8(0)   // attributes -- zero, no compression for the inner message
					wb.writeBytes(msg.Key)
					wb.writeBytes(msg.Value)
				}))
			})
			wb.Write(bs)
		}
	})
	if f.codec != nil {
		bs = newWB().call(func(wb *kafkaWriteBuffer) {
			wb.writeInt64(f.msgs[0].Offset) // offset
			wb.writeBytes(newWB().call(func(wb *kafkaWriteBuffer) {
				compressed := mustCompress(bs, f.codec)
				wb.writeInt32(-1)            // crc, unused
				wb.writeInt8(0)              // magic
				wb.writeInt8(f.codec.Code()) // attributes
				wb.writeBytes(nil)           // key is always nil for compressed
				wb.writeBytes(compressed)    // the value is the compressed message
			}))
		})
	}
	return bs
}

type v1MessageSetBuilder struct {
	msgs  []Message
	codec CompressionCodec
}

func (f v1MessageSetBuilder) messages() []Message {
	return f.msgs
}

func (f v1MessageSetBuilder) bytes() []byte {
	bs := newWB().call(func(wb *kafkaWriteBuffer) {
		for i, msg := range f.msgs {
			bs := newWB().call(func(wb *kafkaWriteBuffer) {
				if f.codec != nil {
					wb.writeInt64(int64(i)) // compressed inner message offsets are relative
				} else {
					wb.writeInt64(msg.Offset) // offset
				}
				wb.writeBytes(newWB().call(func(wb *kafkaWriteBuffer) {
					wb.writeInt32(-1)                     // crc, unused
					wb.writeInt8(1)                       // magic
					wb.writeInt8(0)                       // attributes -- zero, no compression for the inner message
					wb.writeInt64(1000 * msg.Time.Unix()) // timestamp
					wb.writeBytes(msg.Key)
					wb.writeBytes(msg.Value)
				}))
			})
			wb.Write(bs)
		}
	})
	if f.codec != nil {
		bs = newWB().call(func(wb *kafkaWriteBuffer) {
			wb.writeInt64(f.msgs[len(f.msgs)-1].Offset)   // offset of the wrapper message is the last offset of the inner messages
			wb.writeBytes(newWB().call(func(wb *kafkaWriteBuffer) {
				bs := mustCompress(bs, f.codec)
				wb.writeInt32(-1)                           // crc, unused
				wb.writeInt8(1)                             // magic
				wb.writeInt8(f.codec.Code())                // attributes
				wb.writeInt64(1000 * f.msgs[0].Time.Unix()) // timestamp
				wb.writeBytes(nil)                          // key is always nil for compressed
				wb.writeBytes(bs)                           // the value is the compressed message
			}))
		})
	}
	return bs
}

type v2MessageSetBuilder struct {
	msgs  []Message
	codec CompressionCodec
}

func (f v2MessageSetBuilder) messages() []Message {
	return f.msgs
}

func (f v2MessageSetBuilder) bytes() []byte {
	attributes := int16(0)
	if f.codec != nil {
		attributes = int16(f.codec.Code()) // set codec code on attributes
	}
	return newWB().call(func(wb *kafkaWriteBuffer) {
		wb.writeInt64(f.msgs[0].Offset)
		wb.writeBytes(newWB().call(func(wb *kafkaWriteBuffer) {
			wb.writeInt32(0)                            // leader epoch
			wb.writeInt8(2)                             // magic = 2
			wb.writeInt32(0)                            // crc, unused
			wb.writeInt16(attributes)                   // record set attributes
			wb.writeInt32(0)                            // record set last offset delta
			wb.writeInt64(1000 * f.msgs[0].Time.Unix()) // record set first timestamp
			wb.writeInt64(1000 * f.msgs[0].Time.Unix()) // record set last timestamp
			wb.writeInt64(0)                            // record set producer id
			wb.writeInt16(0)                            // record set producer epoch
			wb.writeInt32(0)                            // record set base sequence
			wb.writeInt32(int32(len(f.msgs)))           // record set count
			bs := newWB().call(func(wb *kafkaWriteBuffer) {
				for i, msg := range f.msgs {
					wb.Write(newWB().call(func(wb *kafkaWriteBuffer) {
						bs := newWB().call(func(wb *kafkaWriteBuffer) {
							wb.writeInt8(0)                                               // record attributes, not used here
							wb.writeVarInt(1000 * (time.Now().Unix() - msg.Time.Unix()))  // timestamp
							wb.writeVarInt(int64(i))                                      // offset delta
							wb.writeVarInt(int64(len(msg.Key)))                           // key len
							wb.Write(msg.Key)                                             // key bytes
							wb.writeVarInt(int64(len(msg.Value)))                         // value len
							wb.Write(msg.Value)                                           // value bytes
							wb.writeVarInt(int64(len(msg.Headers)))                       // number of headers
							for _, header := range msg.Headers {
								wb.writeVarInt(int64(len(header.Key)))
								wb.Write([]byte(header.Key))
								wb.writeVarInt(int64(len(header.Value)))
								wb.Write(header.Value)
							}
						})
						wb.writeVarInt(int64(len(bs)))
						wb.Write(bs)
					}))
				}
			})
			if f.codec != nil {
				bs = mustCompress(bs, f.codec)
			}
			wb.Write(bs)
		}))
	})
}

// kafkaWriteBuffer is a write buffer that helps writing fetch responses
type kafkaWriteBuffer struct {
	writeBuffer
	buf bytes.Buffer
}

func newWB() *kafkaWriteBuffer {
	res := kafkaWriteBuffer{}
	res.writeBuffer.w = &res.buf
	return &res
}

func (f *kafkaWriteBuffer) Bytes() []byte {
	return f.buf.Bytes()
}

// call is a convenience method that allows the kafkaWriteBuffer to be used
// in a functional manner. This is helpful when building
// nested structures, as the return value can be fed into
// other fwWB APIs.
func (f *kafkaWriteBuffer) call(cb func(wb *kafkaWriteBuffer)) []byte {
	cb(f)
	bs := f.Bytes()
	if bs == nil {
		bs = []byte{}
	}
	return bs
}

func mustCompress(bs []byte, codec compress.Codec) (res []byte) {
	buf := bytes.Buffer{}
	codecWriter := codec.NewWriter(&buf)
	_, err := io.Copy(codecWriter, bytes.NewReader(bs))
	if err != nil {
		panic(fmt.Errorf("compress: %w", err))
	}
	err = codecWriter.Close()
	if err != nil {
		panic(fmt.Errorf("close codec writer: %w", err))
	}
	res = buf.Bytes()
	return
}
