package lz4

import (
	"bytes"
	"io/ioutil"
	"sync"

	"github.com/pierrec/lz4"
	"github.com/segmentio/kafka-go"
)

var (
	readerPool sync.Pool
	writerPool sync.Pool
)

func init() {
	readerPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewReader(nil)
		},
	}
	writerPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewWriter(nil)
		},
	}

	kafka.RegisterCompressionCodec(func() kafka.CompressionCodec {
		return NewCompressionCodec()
	})
}

type CompressionCodec struct{}

const Code = 3

func NewCompressionCodec() CompressionCodec {
	return CompressionCodec{}
}

// Code implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Code() int8 {
	return Code
}

// Encode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Encode(src []byte) ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Grow(len(src)) // guess a size to avoid repeat allocations.
	writer := writerPool.Get().(*lz4.Writer)
	writer.Reset(&buf)

	_, err := writer.Write(src)
	if err != nil {
		// don't return writer to pool on error.
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		// don't return writer to pool on error.
		return nil, err
	}

	writerPool.Put(writer)

	return buf.Bytes(), err
}

// Decode implements the kafka.CompressionCodec interface.
func (c CompressionCodec) Decode(src []byte) ([]byte, error) {
	reader := readerPool.Get().(*lz4.Reader)
	reader.Reset(bytes.NewReader(src))
	res, err := ioutil.ReadAll(reader)
	// only return the reader to pool if the read was a success.
	if err == nil {
		readerPool.Put(reader)
	}
	return res, err
}
