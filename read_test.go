package kafka

import (
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
		&VarIntTestCase{v: 0, r: 3, tc: []byte{0, 1, 10, 0}},
		&VarIntTestCase{v: -1, r: 3, tc: []byte{1, 1, 10, 0}},
		&VarIntTestCase{v: 1, r: 3, tc: []byte{2, 1, 10, 0}},
		&VarIntTestCase{v: -2, r: 3, tc: []byte{3, 1, 10, 0}},
		&VarIntTestCase{v: 2, r: 3, tc: []byte{4, 1, 10, 0}},
		&VarIntTestCase{v: 64, r: 2, tc: []byte{128, 1, 10, 0}},
		&VarIntTestCase{v: -64, r: 3, tc: []byte{127, 1, 10, 0}},
		&VarIntTestCase{v: -196, r: 2, tc: []byte{135, 3, 10, 0}},
		&VarIntTestCase{v: -24772, r: 1, tc: []byte{135, 131, 3, 0}},
	}

	for _, tc := range testCases {
		rd := bytes.NewReader(tc.tc)
		rb := &readBuffer{r: rd, n: len(tc.tc)}

		v := rb.readVarInt()
		if rb.err != nil {
			t.Errorf("Failure reading varint: %v", rb.err)
		}
		if v != tc.v {
			t.Errorf("Expected %v; got %v", tc.v, v)
		}
		if rb.n != tc.r {
			t.Errorf("Expected remain %v; got %v", tc.r, rb.n)
		}
	}
}

func TestReadVarIntFailing(t *testing.T) {
	testCase := []byte{135, 135}
	rd := bytes.NewReader(testCase)
	rb := &readBuffer{r: rd, n: len(testCase)}

	rb.readVarInt()

	if rb.err != errShortRead {
		t.Errorf("Expected error while parsing var int: %v", rb.err)
	}
}

func TestReadStringArray(t *testing.T) {
	testCases := map[string]struct {
		a []string
	}{
		"nil": {
			a: nil,
		},
		"multiple elements": {
			a: []string{"a", "b", "c"},
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			b := bytes.NewBuffer(nil)
			w := &writeBuffer{w: b}
			w.writeStringArray(test.a)

			r := &readBuffer{r: b, n: b.Len()}
			a := r.readStringArray()

			if !reflect.DeepEqual(test.a, a) {
				t.Errorf("expected %v; got %v", test.a, a)
			}
		})
	}
}

func TestReadMapStringInt32(t *testing.T) {
	testCases := map[string]struct {
		m map[string][]int32
	}{
		"empty": {
			m: map[string][]int32{},
		},
		"single element": {
			m: map[string][]int32{
				"key": {0, 1, 2},
			},
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			b := bytes.NewBuffer(nil)
			w := &writeBuffer{w: b}
			w.writeInt32(int32(len(test.m)))

			for key, values := range test.m {
				w.writeString(key)
				w.writeInt32Array(values)
			}

			r := &readBuffer{r: b, n: b.Len()}
			m := r.readMapStringInt32Array()

			if !reflect.DeepEqual(test.m, m) {
				t.Errorf("expected %#v; got %#v", test.m, m)
			}
		})
	}
}

func TestReadBytes(t *testing.T) {
	t.Run("reads new bytes", func(t *testing.T) {
		a := []byte("\x00\x00\x00\x06foobar")
		r := &readBuffer{
			r: bytes.NewReader(a),
			n: len(a),
		}

		b := r.readBytes()
		if !bytes.Equal(a[4:], b) {
			t.Errorf("byte slices mismatch: %q != %q", a[4:], b)
		}
		if r.err != nil {
			t.Error("unexpected error:", r.err)
		}
		if r.n != 0 {
			t.Error("not all bytes were consumed")
		}
	})

	t.Run("discards bytes when insufficient", func(t *testing.T) {
		a := []byte("\x00\x00\x00\x06foobar")
		r := &readBuffer{
			r: bytes.NewReader(a),
			n: len(a) - 3,
		}

		b := r.readBytes()
		if !bytes.Equal([]byte{}, b) {
			t.Errorf("byte slices mismatch: %q != %q", []byte{}, b)
		}
		if r.err != errShortRead {
			t.Error("unexpected error:", r.err)
		}
		if r.n != 0 {
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

	bb := b1.Bytes()
	bn := b1.Len()

	b2 := bytes.NewReader(bb)
	rb := &readBuffer{r: b2, n: bn}

	for i := 0; i < b.N; i++ {
		v := rb.readVarInt()

		if rb.err != nil {
			b.Fatalf("unexpected error reading a varint from the input: %v", rb.err)
		}

		if rb.n != 0 {
			b.Fatalf("unexpected bytes remaining to be read in the input (%d B)", rb.n)
		}

		if v != N {
			b.Fatalf("value mismatch, expected %d but found %d", N, v)
		}

		b2.Reset(bb)
		rb.n = bn
	}
}
