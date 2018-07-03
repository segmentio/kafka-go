package lz4

import (
	"bytes"
	"testing"
)

func TestLZ4(t *testing.T) {
	payload := []byte("message")
	r1 := make([]byte, 6*len(payload))
	r2 := make([]byte, len(payload))

	c := CompressionCodec{}

	t.Run("encode", func(t *testing.T) {
		n, err := c.Encode(r1, payload)
		if err != nil {
			t.Error(err)
		}
		r1 = r1[:n]
		if bytes.Equal(payload, r1) {
			t.Error("failed to encode payload")
			t.Log("got: ", r1)
		}
	})

	t.Run("decode", func(t *testing.T) {
		n, err := c.Decode(r2, r1)
		if err != nil {
			t.Error(err)
		}
		r2 = r2[:n]
		if !bytes.Equal(payload, r2) {
			t.Error("failed to decode payload")
			t.Log("expected: ", payload)
			t.Log("got: ", r2)
		}
	})
}
