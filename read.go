package kafka

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
)

type readBuffer struct {
	r   io.Reader
	b   [16]byte
	n   int
	err error
}

func (rb *readBuffer) readFull(b []byte) int {
	if rb.err != nil {
		if rb.err == io.EOF {
			rb.err = io.ErrUnexpectedEOF
		}
		return 0
	}
	if rb.n < len(b) {
		rb.discardAll()
		if rb.err == nil {
			rb.err = errShortRead
		}
		return 0
	}
	n, err := io.ReadAtLeast(rb.r, b, len(b))
	rb.n -= n
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		rb.err = err
	}
	return n
}

func (rb *readBuffer) readByte() byte {
	if rb.readFull(rb.b[:1]) == 1 {
		return rb.b[0]
	}
	return 0
}

func (rb *readBuffer) readInt8() int8 {
	return int8(rb.readByte())
}

func (rb *readBuffer) readInt16() int16 {
	if rb.readFull(rb.b[:2]) == 2 {
		return makeInt16(rb.b[:2])
	}
	return 0
}

func (rb *readBuffer) readInt32() int32 {
	if rb.readFull(rb.b[:4]) == 4 {
		return makeInt32(rb.b[:4])
	}
	return 0
}

func (rb *readBuffer) readInt64() int64 {
	if rb.readFull(rb.b[:8]) == 8 {
		return makeInt64(rb.b[:8])
	}
	return 0
}

func (rb *readBuffer) readVarInt() int64 {
	x := uint64(0)
	s := uint(0)

	for {
		b := rb.readByte()

		if b < 0x80 {
			x |= uint64(b) << s
			return int64(x>>1) ^ -(int64(x) & 1)
		}

		x |= uint64(b&0x7F) << s
		s += 7
	}
}

func (rb *readBuffer) read(n int) []byte {
	b := make([]byte, n)
	i := rb.readFull(b)
	return b[:i]
}

func (rb *readBuffer) readBool() bool {
	return rb.readByte() != 0
}

func (rb *readBuffer) readString() string {
	n := rb.readInt16()
	if n < 0 {
		return ""
	}
	return string(rb.read(int(n)))
}

func (rb *readBuffer) readVarString() string {
	n := rb.readVarInt()
	if n < 0 {
		return ""
	}
	return string(rb.read(int(n)))
}

func (rb *readBuffer) readBytes() []byte {
	n := rb.readInt32()
	if n < 0 {
		return nil
	}
	return rb.read(int(n))
}

func (rb *readBuffer) readVarBytes() []byte {
	n := rb.readVarInt()
	if n < 0 {
		return nil
	}
	return rb.read(int(n))
}

func (rb *readBuffer) readStringArray() []string {
	n := int(rb.readInt32())
	a := ([]string)(nil)

	if n > 0 {
		a = make([]string, n)
	}

	for i := 0; i < n; i++ {
		a[i] = rb.readString()
	}

	return a
}

func (rb *readBuffer) readMapStringInt32Array() map[string][]int32 {
	n := int(rb.readInt32())
	m := make(map[string][]int32, n)

	for i := 0; i < n; i++ {
		k := rb.readString()
		c := int(rb.readInt32())
		a := make([]int32, c)

		for j := 0; j < c; j++ {
			a[j] = rb.readInt32()
		}

		m[k] = a
	}

	return m
}

func (rb *readBuffer) readValue(a interface{}) {
	switch v := a.(type) {
	case *int8:
		*v = rb.readInt8()
	case *int16:
		*v = rb.readInt16()
	case *int32:
		*v = rb.readInt32()
	case *int64:
		*v = rb.readInt64()
	case *bool:
		*v = rb.readBool()
	case *string:
		*v = rb.readString()
	case *[]byte:
		*v = rb.readBytes()
	case readable:
		v.readFrom(rb)
	default:
		switch v := reflect.ValueOf(a).Elem(); v.Kind() {
		case reflect.Struct:
			rb.readStruct(v)
		case reflect.Slice:
			rb.readSlice(v)
		default:
			panic(fmt.Sprintf("unsupported type: %T", a))
		}
	}
}

func (rb *readBuffer) readStruct(v reflect.Value) {
	for i, n := 0, v.NumField(); i < n; i++ {
		rb.readValue(v.Field(i).Addr().Interface())
	}
}

func (rb *readBuffer) readSlice(v reflect.Value) {
	n := int(rb.readInt32())

	if n < 0 {
		v.Set(reflect.Zero(v.Type()))
	} else {
		v.Set(reflect.MakeSlice(v.Type(), n, n))

		for i := 0; i < n; i++ {
			rb.readValue(v.Index(i).Addr().Interface())
		}
	}
}

func (rb *readBuffer) readArray(f func()) {
	n := int(rb.readInt32())

	for i := 0; i < n; i++ {
		f()
	}
}

func (rb *readBuffer) Read(b []byte) (int, error) {
	if rb.err != nil {
		return 0, rb.err
	}
	if rb.n == 0 {
		return 0, io.EOF
	}
	n := len(b)
	if n > rb.n {
		n = rb.n
	}
	n, err := rb.r.Read(b)
	rb.n -= n
	rb.err = err
	return n, err
}

func (rb *readBuffer) discardBytes() {
	rb.discard(int(rb.readInt32()))
}

func (rb *readBuffer) discardString() {
	rb.discard(int(rb.readInt16()))
}

func (rb *readBuffer) discardAll() {
	rb.discard(rb.n)
}

func (rb *readBuffer) discard(n int) int {
	if rb.err != nil {
		return 0
	}
	if rb.n < n {
		n, rb.err = rb.n, errShortRead
	}
	n, err := discard(rb.r, n)
	rb.n -= n
	if err != nil {
		rb.err = err
	}
	return n
}

func discard(r io.Reader, n int) (d int, err error) {
	if n > 0 {
		switch x := r.(type) {
		case *readBuffer:
			d = x.discard(n)
		case *bufio.Reader:
			d, err = x.Discard(n)
		default:
			var c int64
			c, err = io.CopyN(ioutil.Discard, x, int64(n))
			d = int(c)
		}
		switch {
		case d < n && err == nil:
			err = io.ErrUnexpectedEOF
		case d == n && err != nil:
			err = nil
		}
	}
	return
}

type readable interface {
	readFrom(*readBuffer)
}

var errShortRead = errors.New("not enough bytes available to load the response")

func (rb *readBuffer) readFetchResponseHeaderV2() (throttle int32, watermark int64, err error) {
	var n int32
	var p struct {
		Partition           int32
		ErrorCode           int16
		HighwaterMarkOffset int64
		MessageSetSize      int32
	}

	throttle = rb.readInt32()
	n = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka topic was expected in the fetch response but the client received %d", n)
		return
	}

	// We ignore the topic name because we've requests messages for a single
	// topic, unless there's a bug in the kafka server we will have received
	// the name of the topic that we requested.
	rb.discardString()
	n = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka partition was expected in the fetch response but the client received %d", n)
		return
	}

	rb.readValue(&p)

	if p.ErrorCode != 0 {
		err = Error(p.ErrorCode)
		return
	}

	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if rb.n != int(p.MessageSetSize) {
		err = fmt.Errorf("the size of the message set in a fetch response doesn't match the number of remaining bytes (message set size = %d, remaining bytes = %d)", p.MessageSetSize, rb.n)
		return
	}

	watermark, err = p.HighwaterMarkOffset, rb.err
	return
}

type abortedTransaction struct {
	ProducerId  int64
	FirstOffset int64
}

func (rb *readBuffer) readFetchResponseHeaderV5() (throttle int32, watermark int64, err error) {
	var n int32
	var p struct {
		Partition           int32
		ErrorCode           int16
		HighwaterMarkOffset int64
		LastStableOffset    int64
		LogStartOffset      int64
	}

	throttle = rb.readInt32()
	n = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka topic was expected in the fetch response but the client received %d", n)
		return
	}

	// We ignore the topic name because we've requests messages for a single
	// topic, unless there's a bug in the kafka server we will have received
	// the name of the topic that we requested.
	rb.discardString()
	n = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka partition was expected in the fetch response but the client received %d", n)
		return
	}

	rb.readValue(&p)

	if p.ErrorCode != 0 {
		err = Error(p.ErrorCode)
		return
	}

	var abortedTransactions []abortedTransaction
	var abortedTransactionCount = int(rb.readInt32())

	if abortedTransactionCount < 0 {
		abortedTransactions = nil
	} else {
		abortedTransactions = make([]abortedTransaction, abortedTransactionCount)

		for i := 0; i < abortedTransactionCount; i++ {
			rb.readValue(&abortedTransactions[i])
		}
	}

	var messageSetSize = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if rb.n != int(messageSetSize) {
		err = fmt.Errorf("the size of the message set in a fetch response doesn't match the number of remaining bytes (message set size = %d, remaining bytes = %d)", messageSetSize, rb.n)
		return
	}

	watermark, err = p.HighwaterMarkOffset, rb.err
	return

}

func (rb *readBuffer) readFetchResponseHeaderV10() (throttle int32, watermark int64, err error) {
	var n int32
	var p struct {
		Partition           int32
		ErrorCode           int16
		HighwaterMarkOffset int64
		LastStableOffset    int64
		LogStartOffset      int64
	}

	throttle = rb.readInt32()

	if errorCode := rb.readInt16(); errorCode != 0 {
		err = Error(errorCode)
		return
	}

	rb.readInt32()
	n = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka topic was expected in the fetch response but the client received %d", n)
		return
	}

	// We ignore the topic name because we've requests messages for a single
	// topic, unless there's a bug in the kafka server we will have received
	// the name of the topic that we requested.
	rb.discardString()
	n = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka partition was expected in the fetch response but the client received %d", n)
		return
	}

	rb.readValue(&p)

	if p.ErrorCode != 0 {
		err = Error(p.ErrorCode)
		return
	}

	var abortedTransactionCount = int(rb.readInt32())
	var abortedTransactions []abortedTransaction

	if abortedTransactionCount < 0 {
		abortedTransactions = nil
	} else {
		abortedTransactions = make([]abortedTransaction, abortedTransactionCount)

		for i := 0; i < abortedTransactionCount; i++ {
			rb.readValue(&abortedTransactions[i])
		}
	}

	var messageSetSize = rb.readInt32()
	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if rb.n != int(messageSetSize) {
		err = fmt.Errorf("the size of the message set in a fetch response doesn't match the number of remaining bytes (message set size = %d, remaining bytes = %d)", messageSetSize, rb.n)
		return
	}

	watermark, err = p.HighwaterMarkOffset, rb.err
	return

}

func (rb *readBuffer) readMessageHeader() (offset int64, attributes int8, timestamp int64, err error) {
	offset = rb.readInt64()
	// On discarding the message size and CRC:
	// ---------------------------------------
	//
	// - Not sure why kafka gives the message size here, we already have the
	// number of remaining bytes in the response and kafka should only truncate
	// the trailing message.
	//
	// - TCP is already taking care of ensuring data integrity, no need to
	// waste resources doing it a second time so we just skip the message CRC.
	//
	rb.readInt64()
	version := rb.readInt8()
	attributes = rb.readInt8()

	switch version {
	case 0:
	case 1:
		timestamp = rb.readInt64()
	default:
		err = fmt.Errorf("unsupported message version %d found in fetch response", version)
	}

	return
}
