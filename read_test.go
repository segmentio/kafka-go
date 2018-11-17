package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestReadVarInt(t *testing.T) {
	//buf := []byte{127, 10, 10, 0}
	testCases := map[int64][]byte{
		128:   []byte{128, 1, 10, 0},
		127:   []byte{127, 1, 10, 0},
		391:   []byte{135, 3, 10, 0},
		49543: []byte{135, 131, 3, 0},
	}

	for expectedValue, testCase := range testCases {
		var v int64
		rd := bufio.NewReader(bytes.NewReader(testCase))
		_, err := readVarInt(rd, len(testCase), &v)
		if err != nil {
			t.Errorf("Failure during reading: %v", err)
		}
		if v != expectedValue {
			t.Errorf("Expected %v; got %v", expectedValue, v)
		}
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
			buf := bytes.NewBuffer(nil)

			w := bufio.NewWriter(buf)
			writeStringArray(w, test.Value)
			w.Flush()

			var actual []string
			readStringArray(bufio.NewReader(buf), buf.Len(), &actual)
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
			buf := bytes.NewBuffer(nil)

			w := bufio.NewWriter(buf)
			writeInt32(w, int32(len(test.Data)))
			for key, values := range test.Data {
				writeString(w, key)
				writeInt32Array(w, values)
			}
			w.Flush()

			var actual map[string][]int32
			readMapStringInt32(bufio.NewReader(buf), buf.Len(), &actual)
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
