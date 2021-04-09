package prototest

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/apoorvag-mav/kafka-go/protocol"
)

func TestRequest(t *testing.T, version int16, msg protocol.Message) {
	reset := load(msg)

	t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
		b := &bytes.Buffer{}

		if err := protocol.WriteRequest(b, version, 1234, "me", msg); err != nil {
			t.Fatal(err)
		}

		reset()

		t.Logf("\n%s\n", hex.Dump(b.Bytes()))

		apiVersion, correlationID, clientID, req, err := protocol.ReadRequest(b)
		if err != nil {
			t.Fatal(err)
		}
		if apiVersion != version {
			t.Errorf("api version mismatch: %d != %d", apiVersion, version)
		}
		if correlationID != 1234 {
			t.Errorf("correlation id mismatch: %d != %d", correlationID, 1234)
		}
		if clientID != "me" {
			t.Errorf("client id mismatch: %q != %q", clientID, "me")
		}
		if !deepEqual(msg, req) {
			t.Errorf("request message mismatch:")
			t.Logf("expected: %+v", msg)
			t.Logf("found:    %+v", req)
		}
	})
}

func BenchmarkRequest(b *testing.B, version int16, msg protocol.Message) {
	reset := load(msg)

	b.Run(fmt.Sprintf("v%d", version), func(b *testing.B) {
		buffer := &bytes.Buffer{}
		buffer.Grow(1024)

		b.Run("read", func(b *testing.B) {
			w := io.Writer(buffer)

			if err := protocol.WriteRequest(w, version, 1234, "client", msg); err != nil {
				b.Fatal(err)
			}

			reset()

			p := buffer.Bytes()
			x := bytes.NewReader(p)
			r := bufio.NewReader(x)

			for i := 0; i < b.N; i++ {
				_, _, _, req, err := protocol.ReadRequest(r)
				if err != nil {
					b.Fatal(err)
				}
				closeMessage(req)
				x.Reset(p)
				r.Reset(x)
			}

			b.SetBytes(int64(len(p)))
			buffer.Reset()
		})

		b.Run("write", func(b *testing.B) {
			w := io.Writer(buffer)
			n := int64(0)

			for i := 0; i < b.N; i++ {
				if err := protocol.WriteRequest(w, version, 1234, "client", msg); err != nil {
					b.Fatal(err)
				}
				reset()
				n = int64(buffer.Len())
				buffer.Reset()
			}

			b.SetBytes(n)
		})
	})
}
