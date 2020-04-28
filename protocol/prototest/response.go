package prototest

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
)

func TestResponse(t *testing.T, version int16, msg protocol.Message) {
	defer closeMessage(msg)

	t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
		b := &bytes.Buffer{}
		r := bufio.NewReader(b)
		w := bufio.NewWriter(b)

		if err := protocol.WriteResponse(w, version, 1234, msg); err != nil {
			t.Fatal(err)
		}

		correlationID, res, err := protocol.ReadResponse(r, int16(msg.ApiKey()), version)
		if err != nil {
			t.Fatal(err)
		}
		if correlationID != 1234 {
			t.Errorf("correlation id mismatch: %d != %d", correlationID, 1234)
		}
		if !deepEqual(msg, res) {
			t.Errorf("response message mismatch:")
			t.Logf("expected: %+v", msg)
			t.Logf("found:    %+v", res)
		}
		closeMessage(res)
	})
}

func BenchmarkResponse(b *testing.B, version int16, msg protocol.Message) {
	defer closeMessage(msg)

	b.Run(fmt.Sprintf("v%d", version), func(b *testing.B) {
		apiKey := int16(msg.ApiKey())
		buffer := &bytes.Buffer{}
		buffer.Grow(1024)

		b.Run("read", func(b *testing.B) {
			w := bufio.NewWriter(buffer)

			if err := protocol.WriteResponse(w, version, 1234, msg); err != nil {
				b.Fatal(err)
			}

			p := buffer.Bytes()
			x := bytes.NewReader(p)
			r := bufio.NewReader(x)

			for i := 0; i < b.N; i++ {
				_, res, err := protocol.ReadResponse(r, apiKey, version)
				if err != nil {
					b.Fatal(err)
				}
				closeMessage(res)
				x.Reset(p)
				r.Reset(x)
			}

			b.SetBytes(int64(len(p)))
			buffer.Reset()
		})

		b.Run("write", func(b *testing.B) {
			w := bufio.NewWriter(buffer)
			n := int64(0)

			for i := 0; i < b.N; i++ {
				if err := protocol.WriteResponse(w, version, 1234, msg); err != nil {
					b.Fatal(err)
				}
				n = int64(buffer.Len())
				buffer.Reset()
			}

			b.SetBytes(n)
		})
	})
}
