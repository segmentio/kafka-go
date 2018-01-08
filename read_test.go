package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

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
