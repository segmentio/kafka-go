package protocol

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

// newTestDecoder builds a decoder over data, with remain set to len(data), the
// same invariant Unmarshal establishes before decoding a response frame.
func newTestDecoder(data []byte) *decoder {
	d := &decoder{reader: bytes.NewReader(data)}
	d.remain = len(data)
	return d
}

// TestDecodeArrayMalformedLengthDoesNotOOM reproduces a malformed or
// non-Kafka response whose declared array length vastly exceeds the bytes
// actually available in the frame. Before the length is bounded, makeArray
// allocates directly from the untrusted length field, so a tiny buffer
// claiming billions of elements can exhaust memory before a single element
// is decoded.
func TestDecodeArrayMalformedLengthDoesNotOOM(t *testing.T) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(1<<31-1)) // math.MaxInt32 elements
	d := newTestDecoder(buf)

	p := new([]int32)
	d.decodeArray(valueOf(p), reflect.TypeOf(int32(0)), (*decoder).decodeInt32)

	if d.remain != 0 {
		t.Fatalf("expected all bytes consumed, got remain=%d", d.remain)
	}
	if n := len(*p); n > len(buf) {
		t.Fatalf("array length %d was not bounded to the %d bytes available in the frame", n, len(buf))
	}
}

// TestDecodeCompactArrayMalformedLengthDoesNotOOM is the compact-encoding
// (flexible version) counterpart of TestDecodeArrayMalformedLengthDoesNotOOM.
func TestDecodeCompactArrayMalformedLengthDoesNotOOM(t *testing.T) {
	buf := []byte{0xff, 0xff, 0xff, 0xff, 0x0f} // unsigned varint for MaxUint32
	d := newTestDecoder(buf)

	p := new([]int32)
	d.decodeCompactArray(valueOf(p), reflect.TypeOf(int32(0)), (*decoder).decodeInt32)

	if n := len(*p); n > len(buf) {
		t.Fatalf("array length %d was not bounded to the %d bytes available in the frame", n, len(buf))
	}
}

func TestBoundArrayLen(t *testing.T) {
	tests := []struct {
		n, remain, want int
	}{
		{n: 0, remain: 10, want: 0},
		{n: 5, remain: 10, want: 5},
		{n: 10, remain: 10, want: 10},
		{n: 11, remain: 10, want: 10},
		{n: 1 << 30, remain: 3, want: 3},
		{n: 5, remain: 0, want: 0},
	}
	for _, test := range tests {
		if got := boundArrayLen(test.n, test.remain); got != test.want {
			t.Errorf("boundArrayLen(%d, %d) = %d, want %d", test.n, test.remain, got, test.want)
		}
	}
}
