package kafka

import (
	"testing"
)

func TestCompression(t *testing.T) {
	msg := Message{
		Value: []byte("message"),
	}

	testEncodeDecode(t, msg, CompressionNone)
	testEncodeDecode(t, msg, CompressionGZIP)
	testEncodeDecode(t, msg, CompressionSnappy)
}

func testEncodeDecode(t *testing.T, m Message, codec CompressionCodec) {
	var encoded Message

	t.Run("encode with "+codec.String(), func(t *testing.T) {
		var err error

		m.CompressionCodec = codec
		encoded, err = m.encode()
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("decode with "+codec.String(), func(t *testing.T) {
		decoded, err := encoded.decode()
		if err != nil {
			t.Error(err)
		}
		if string(decoded.Value) != "message" {
			t.Error("bad message")
			t.Log("got: ", string(decoded.Value))
			t.Log("expected: message")
		}
	})
}
