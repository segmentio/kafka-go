package kafka

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"math"
	"reflect"
	"testing"
)

type VarIntTestCase struct {
	v  int64
	r  int
	tc []byte
}

func TestReadVarInt(t *testing.T) {
	testCases := []*VarIntTestCase{
		{v: 0, r: 3, tc: []byte{0, 1, 10, 0}},
		{v: -1, r: 3, tc: []byte{1, 1, 10, 0}},
		{v: 1, r: 3, tc: []byte{2, 1, 10, 0}},
		{v: -2, r: 3, tc: []byte{3, 1, 10, 0}},
		{v: 2, r: 3, tc: []byte{4, 1, 10, 0}},
		{v: 64, r: 2, tc: []byte{128, 1, 10, 0}},
		{v: -64, r: 3, tc: []byte{127, 1, 10, 0}},
		{v: -196, r: 2, tc: []byte{135, 3, 10, 0}},
		{v: -24772, r: 1, tc: []byte{135, 131, 3, 0}},
	}

	for _, tc := range testCases {
		var v int64
		rd := bufio.NewReader(bytes.NewReader(tc.tc))
		remain, err := readVarInt(rd, len(tc.tc), &v)
		if err != nil {
			t.Errorf("Failure during reading: %v", err)
		}
		if v != tc.v {
			t.Errorf("Expected %v; got %v", tc.v, v)
		}
		if remain != tc.r {
			t.Errorf("Expected remain %v; got %v", tc.r, remain)
		}
	}
}

func TestReadVarIntFailing(t *testing.T) {
	var v int64
	testCase := []byte{135, 135}
	rd := bufio.NewReader(bytes.NewReader(testCase))
	_, err := readVarInt(rd, len(testCase), &v)
	if err != errShortRead {
		t.Errorf("Expected error while parsing var int: %v", err)
	}
}

func TestReadStringArray(t *testing.T) {
	testCases := map[string]struct {
		Value []string
	}{
		"nil": {
			Value: nil,
		},
		"multiple elements": {
			Value: []string{"a", "b", "c"},
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			b := bytes.NewBuffer(nil)
			w := &writeBuffer{w: b}
			w.writeStringArray(test.Value)

			var actual []string
			readStringArray(bufio.NewReader(b), b.Len(), &actual)
			if !reflect.DeepEqual(test.Value, actual) {
				t.Errorf("expected %v; got %v", test.Value, actual)
			}
		})
	}
}

func TestReadMapStringInt32(t *testing.T) {
	testCases := map[string]struct {
		Data map[string][]int32
	}{
		"empty": {
			Data: map[string][]int32{},
		},
		"single element": {
			Data: map[string][]int32{
				"key": {0, 1, 2},
			},
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			b := bytes.NewBuffer(nil)
			w := &writeBuffer{w: b}
			w.writeInt32(int32(len(test.Data)))

			for key, values := range test.Data {
				w.writeString(key)
				w.writeInt32Array(values)
			}

			var actual map[string][]int32
			readMapStringInt32(bufio.NewReader(b), b.Len(), &actual)
			if !reflect.DeepEqual(test.Data, actual) {
				t.Errorf("expected %#v; got %#v", test.Data, actual)
			}
		})
	}
}

func TestReadNewBytes(t *testing.T) {

	t.Run("reads new bytes", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewReader([]byte("foobar")))

		b, remain, err := readNewBytes(r, 6, 3)
		if string(b) != "foo" {
			t.Error("should have returned 3 bytes")
		}
		if remain != 3 {
			t.Error("should have calculated remaining correctly")
		}
		if err != nil {
			t.Error("should not have errored")
		}

		b, remain, err = readNewBytes(r, remain, 3)
		if string(b) != "bar" {
			t.Error("should have returned 3 bytes")
		}
		if remain != 0 {
			t.Error("should have calculated remaining correctly")
		}
		if err != nil {
			t.Error("should not have errored")
		}

		b, err = r.Peek(0)
		if len(b) > 0 {
			t.Error("not all bytes were consumed")
		}
	})

	t.Run("discards bytes when insufficient", func(t *testing.T) {
		r := bufio.NewReader(bytes.NewReader([]byte("foo")))
		b, remain, err := readNewBytes(bufio.NewReader(r), 3, 4)
		if string(b) != "foo" {
			t.Error("should have returned available bytes")
		}
		if remain != 0 {
			t.Error("all bytes should have been consumed")
		}
		if err != errShortRead {
			t.Error("should have returned errShortRead")
		}
		b, err = r.Peek(0)
		if len(b) > 0 {
			t.Error("not all bytes were consumed")
		}
	})
}

func BenchmarkWriteVarInt(b *testing.B) {
	wb := &writeBuffer{w: ioutil.Discard}

	for i := 0; i < b.N; i++ {
		wb.writeVarInt(math.MaxInt64)
	}
}

func BenchmarkReadVarInt(b *testing.B) {
	b1 := new(bytes.Buffer)
	wb := &writeBuffer{w: b1}

	const N = math.MaxInt64
	wb.writeVarInt(N)

	b2 := bytes.NewReader(b1.Bytes())
	rb := bufio.NewReader(b2)
	n := b1.Len()

	for i := 0; i < b.N; i++ {
		v := int64(0)
		r, err := readVarInt(rb, n, &v)

		if err != nil {
			b.Fatalf("unexpected error reading a varint from the input: %v", err)
		}

		if r != 0 {
			b.Fatalf("unexpected bytes remaining to be read in the input (%d B)", r)
		}

		if v != N {
			b.Fatalf("value mismatch, expected %d but found %d", N, v)
		}

		b2.Reset(b1.Bytes())
		rb.Reset(b2)
	}
}
