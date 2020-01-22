package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func testRequest(t *testing.T, version int16, msg Message) {
	defer closeMessage(msg)

	t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
		b := &bytes.Buffer{}
		r := bufio.NewReader(b)
		w := bufio.NewWriter(b)

		if err := WriteRequest(w, version, 1234, "me", msg); err != nil {
			t.Fatal(err)
		}

		apiVersion, correlationID, clientID, req, err := ReadRequest(r)
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
		closeMessage(req)
	})
}

func benchmarkRequest(b *testing.B, version int16, msg Message) {
	defer closeMessage(msg)

	b.Run(fmt.Sprintf("v%d", version), func(b *testing.B) {
		buffer := &bytes.Buffer{}
		buffer.Grow(1024)

		b.Run("read", func(b *testing.B) {
			w := bufio.NewWriter(buffer)

			if err := WriteRequest(w, version, 1234, "client", msg); err != nil {
				b.Fatal(err)
			}

			p := buffer.Bytes()
			x := bytes.NewReader(p)
			r := bufio.NewReader(x)

			for i := 0; i < b.N; i++ {
				_, _, _, req, err := ReadRequest(r)
				if err != nil {
					b.Fatal(err)
				}
				closeMessage(req)
				x.Reset(p)
				r.Reset(x)
			}

			b.SetBytes(int64(len(p)))
		})

		b.Run("write", func(b *testing.B) {
			w := bufio.NewWriter(buffer)
			n := int64(0)

			for i := 0; i < b.N; i++ {
				if err := WriteRequest(w, version, 1234, "client", msg); err != nil {
					b.Fatal(err)
				}
				n = int64(buffer.Len())
				buffer.Reset()
			}

			b.SetBytes(n)
		})
	})
}
