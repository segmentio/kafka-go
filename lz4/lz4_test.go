package lz4

import (
	"bytes"
	"testing"
)

func TestLZ4(t *testing.T) {
	var r1, r2 []byte
	var err error
	payload := []byte("message")

	t.Run("encode", func(t *testing.T) {
		r1, err = Encode(payload, 1)
		if err != nil {
			t.Error(err)
		}
		if bytes.Equal(payload, r1) {
			t.Error("failed to encode payload")
			t.Log("got: ", r1)
		}
	})

	t.Run("decode", func(t *testing.T) {
		r2, err = Decode(r1)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(payload, r2) {
			t.Error("failed to decode payload")
			t.Log("expected: ", payload)
			t.Log("got: ", r2)
		}
	})
}
