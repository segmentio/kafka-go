package kafka

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"reflect"
)

var errShortRead = errors.New("not enough bytes available to load the response")

func peekRead(r *bufio.Reader, sz int, n int, f func([]byte)) (int, error) {
	if n > sz {
		return sz, errShortRead
	}
	b, err := r.Peek(n)
	if err != nil {
		return sz, err
	}
	f(b)
	return discardN(r, sz, n)
}

func readInt8(r *bufio.Reader, sz int, v *int8) (int, error) {
	return peekRead(r, sz, 1, func(b []byte) { *v = makeInt8(b) })
}

func readInt16(r *bufio.Reader, sz int, v *int16) (int, error) {
	return peekRead(r, sz, 2, func(b []byte) { *v = makeInt16(b) })
}

func readInt32(r *bufio.Reader, sz int, v *int32) (int, error) {
	return peekRead(r, sz, 4, func(b []byte) { *v = makeInt32(b) })
}

func readInt64(r *bufio.Reader, sz int, v *int64) (int, error) {
	return peekRead(r, sz, 8, func(b []byte) { *v = makeInt64(b) })
}

func readString(r *bufio.Reader, sz int, v *string) (int, error) {
	return readStringWith(r, sz, func(r *bufio.Reader, sz int, len int16) (int, error) {
		var b []byte
		var n = int(len)

		if n > sz {
			return sz, errShortRead
		}
		b = make([]byte, n)

		n, err := io.ReadFull(r, b)
		if err == nil {
			*v = string(b)
		}

		return sz - n, err
	})
}

func readStringWith(r *bufio.Reader, sz int, cb func(*bufio.Reader, int, int16) (int, error)) (int, error) {
	var err error
	var len int16

	if sz, err = readInt16(r, sz, &len); err != nil {
		return sz, err
	}

	if len < 0 {
		len = 0
	}

	if sz < int(len) {
		return sz, errShortRead
	}

	return cb(r, sz, len)
}

func readBytes(r *bufio.Reader, sz int, v *[]byte) (int, error) {
	return readStringWith(r, sz, func(r *bufio.Reader, sz int, len int16) (int, error) {
		var b []byte
		var n = int(len)

		if n > sz {
			return sz, errShortRead
		}
		b = make([]byte, n)

		n, err := io.ReadFull(r, b)
		if err == nil {
			*v = b
		}

		return sz - n, err
	})
}

func readBytesWith(r *bufio.Reader, sz int, cb func(*bufio.Reader, int, int32) (int, error)) (int, error) {
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	if len < 0 {
		len = 0
	}

	if sz < int(len) {
		return sz, errShortRead
	}

	return cb(r, sz, len)
}

func readArrayWith(r *bufio.Reader, sz int, cb func(*bufio.Reader, int) (int, error)) (int, error) {
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	for n := int(len); n > 0; n-- {
		if sz, err = cb(r, sz); err != nil {
			break
		}
	}

	return sz, err
}

func read(r *bufio.Reader, sz int, a interface{}) (int, error) {
	switch v := a.(type) {
	case *int8:
		return readInt8(r, sz, v)
	case *int16:
		return readInt16(r, sz, v)
	case *int32:
		return readInt32(r, sz, v)
	case *int64:
		return readInt64(r, sz, v)
	case *string:
		return readString(r, sz, v)
	case *[]byte:
		return readBytes(r, sz, v)
	}
	switch v := reflect.ValueOf(a).Elem(); v.Kind() {
	case reflect.Struct:
		return readStruct(r, sz, v)
	case reflect.Slice:
		return readSlice(r, sz, v)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func readStruct(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var err error
	for i, n := 0, v.NumField(); i != n; i++ {
		if sz, err = read(r, sz, v.Field(i).Addr().Interface()); err != nil {
			return sz, err
		}
	}
	return sz, nil
}

func readSlice(r *bufio.Reader, sz int, v reflect.Value) (int, error) {
	var err error
	var len int32

	if sz, err = readInt32(r, sz, &len); err != nil {
		return sz, err
	}

	if n := int(len); n < 0 {
		v.Set(reflect.Zero(v.Type()))
	} else {
		v.Set(reflect.MakeSlice(v.Type(), n, n))

		for i := 0; i != n; i++ {
			if sz, err = read(r, sz, v.Index(i).Addr().Interface()); err != nil {
				return sz, err
			}
		}
	}

	return sz, nil
}

func readFetchResponseHeader(r *bufio.Reader, size int) (throttle int32, remain int, err error) {
	var n int32
	var p struct {
		Partition           int32
		ErrorCode           int16
		HighwaterMarkOffset int64
		MessageSetSize      int32
	}

	if remain, err = readInt32(r, size, &throttle); err != nil {
		return
	}

	if remain, err = readInt32(r, remain, &n); err != nil {
		return
	}

	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka topic was expected in the fetch response but the client received %d", n)
		return
	}

	// We ignore the topic name because we've requests messages for a single
	// topic, unless there's a bug in the kafka server we will have received
	// the name of the topic that we requested.
	if remain, err = discardString(r, remain); err != nil {
		return
	}

	if remain, err = readInt32(r, remain, &n); err != nil {
		return
	}

	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if n != 1 {
		err = fmt.Errorf("1 kafka partition was expected in the fetch response but the client received %d", n)
		return
	}

	if remain, err = read(r, remain, &p); err != nil {
		return
	}

	if p.ErrorCode != 0 {
		err = Error(p.ErrorCode)
		return
	}

	// This error should never trigger, unless there's a bug in the kafka client
	// or server.
	if remain != int(p.MessageSetSize) {
		err = fmt.Errorf("the size of the message set in a fetch response doesn't match the number of remaining bytes (message set size = %d, remaining bytes = %d)", p.MessageSetSize, remain)
		return
	}

	return
}

func readMessageHeader(r *bufio.Reader, sz int) (offset int64, attributes int8, timestamp int64, remain int, err error) {
	var version int8
	var msgsize int32

	if remain, err = readInt64(r, sz, &offset); err != nil {
		return
	}

	// Not sure why kafka gives the message size here, we already have the
	// number of remaining bytes in the response and kafka should only truncate
	// the trailing message. We still do a consistency check in case the server
	// returns something weird.
	if remain, err = readInt32(r, remain, &msgsize); err != nil {
		return
	}

	if msgsize < 0 {
		err = fmt.Errorf("invalid negative message size of %d in the fetch response", msgsize)
		return
	}

	if int(msgsize) > remain {
		err = fmt.Errorf("a message size of %d bytes in the fetch response exceeds the remaining size of %d bytes", msgsize, remain)
		return
	}

	// TCP is already taking care of ensuring data integrirty, no need to waste
	// resources doing it a second time so we just skip the message CRC.
	if remain, err = discardInt32(r, remain); err != nil {
		return
	}

	if remain, err = readInt8(r, remain, &version); err != nil {
		return
	}

	if remain, err = readInt8(r, remain, &attributes); err != nil {
		return
	}

	switch version {
	case 0:
	case 1:
		remain, err = readInt64(r, remain, &timestamp)
	default:
		err = fmt.Errorf("unsupported message version %d found in fetch response", version)
	}

	return
}

func readMessageBytes(r *bufio.Reader, sz int, read func(*bufio.Reader, int, int) (int, error)) (int, error) {
	return readBytesWith(r, sz, func(r *bufio.Reader, sz int, len int32) (int, error) {
		return read(r, sz, int(len))
	})
}

func readMessage(r *bufio.Reader, sz int,
	key func(*bufio.Reader, int, int) (int, error),
	val func(*bufio.Reader, int, int) (int, error),
) (offset int64, attributes int8, timestamp int64, remain int, err error) {
	if offset, attributes, timestamp, remain, err = readMessageHeader(r, sz); err != nil {
		return
	}
	if remain, err = readMessageBytes(r, remain, key); err != nil {
		return
	}
	remain, err = readMessageBytes(r, remain, val)
	return
}

func readResponse(r *bufio.Reader, sz int, res interface{}) error {
	sz, err := read(r, sz, res)
	switch err.(type) {
	case Error:
		sz, err = discardN(r, sz, sz)
	}
	return expectZeroSize(sz, err)
}
