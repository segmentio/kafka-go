package kafka

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"
)

// TestWriteFetchRequestV11_ContainsRackID asserts that writeFetchRequestV11
// actually places the rack id into the on-wire request body, and that the v10
// writer does not (so we don't accidentally regress the older path).
func TestWriteFetchRequestV11_ContainsRackID(t *testing.T) {
	const (
		correlationID = 1
		clientID      = "client"
		topic         = "topic-x"
		partition     = int32(0)
		offset        = int64(0)
		minBytes      = 1
		maxBytes      = 1024
		maxWait       = 100 * time.Millisecond
		isoLevel      = int8(0)
		rack          = "rack-nl"
	)

	v11Buf := &bytes.Buffer{}
	v11WB := &writeBuffer{w: v11Buf}
	if err := v11WB.writeFetchRequestV11(correlationID, clientID, topic, partition, offset, minBytes, maxBytes, maxWait, isoLevel, rack); err != nil {
		t.Fatalf("writeFetchRequestV11: %v", err)
	}
	if !bytes.Contains(v11Buf.Bytes(), []byte(rack)) {
		t.Fatalf("v11 request bytes do not contain rack id %q", rack)
	}

	v10Buf := &bytes.Buffer{}
	v10WB := &writeBuffer{w: v10Buf}
	if err := v10WB.writeFetchRequestV10(correlationID, clientID, topic, partition, offset, minBytes, maxBytes, maxWait, isoLevel); err != nil {
		t.Fatalf("writeFetchRequestV10: %v", err)
	}
	if bytes.Contains(v10Buf.Bytes(), []byte(rack)) {
		t.Fatalf("v10 request bytes unexpectedly contain rack id %q", rack)
	}
}

// TestReadFetchResponseHeaderV11_DecodesPreferredReadReplica builds a minimal
// v11 fetch response by hand and verifies that the decoder recovers the
// preferred read replica id and defaults to -1 when the broker reports no
// preference.
func TestReadFetchResponseHeaderV11_DecodesPreferredReadReplica(t *testing.T) {
	cases := []struct {
		name string
		prr  int32
	}{
		{"explicit-preferred-replica", 7},
		{"no-preference-minus-one", -1},
		// broker id 0 is a valid id; make sure the decoder returns it
		// verbatim and doesn't conflate it with "no preference".
		{"broker-id-zero", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := buildFetchResponseV11(t, tc.prr)
			r := bufio.NewReader(bytes.NewReader(buf))
			throttle, watermark, prr, remain, err := readFetchResponseHeaderV11(r, len(buf))
			if err != nil {
				t.Fatalf("readFetchResponseHeaderV11: %v", err)
			}
			if throttle != 0 {
				t.Errorf("throttle: got %d, want 0", throttle)
			}
			if watermark != 1000 {
				t.Errorf("watermark: got %d, want 1000", watermark)
			}
			if prr != tc.prr {
				t.Errorf("preferredReadReplica: got %d, want %d", prr, tc.prr)
			}
			if remain != 0 {
				t.Errorf("remain: got %d, want 0 (no record set bytes left)", remain)
			}
		})
	}
}

// buildFetchResponseV11 hand-assembles a v11 fetch response body containing one
// topic and one partition with the given preferred read replica and an empty
// record set. The shape mirrors what the broker would put on the wire so the
// decoder gets exercised end-to-end.
func buildFetchResponseV11(t *testing.T, preferredReadReplica int32) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	wb := &writeBuffer{w: buf}

	wb.writeInt32(0)  // throttle time ms
	wb.writeInt16(0)  // top-level error code (v7+)
	wb.writeInt32(-1) // session id (v7+)

	// topics array
	wb.writeArrayLen(1)
	wb.writeString("topic-x")

	// partitions array
	wb.writeArrayLen(1)
	wb.writeInt32(0)    // partition index
	wb.writeInt16(0)    // error code
	wb.writeInt64(1000) // high watermark
	wb.writeInt64(900)  // last stable offset
	wb.writeInt64(0)    // log start offset
	wb.writeArrayLen(0) // aborted transactions = empty

	// preferred read replica (KIP-392)
	wb.writeInt32(preferredReadReplica)

	// message set size = 0 (empty record set)
	wb.writeInt32(0)

	return buf.Bytes()
}

// TestReadFetchResponseHeaderV11_NoPreferenceDefault asserts the decoder
// initializes preferredReadReplica to -1 before reading anything from the
// wire, so a malformed/short response surfaces a sensible default rather
// than an accidental "broker 0 is preferred" value.
func TestReadFetchResponseHeaderV11_NoPreferenceDefault(t *testing.T) {
	// A truncated response (just the throttle bytes) should fail to decode
	// but the returned prr should still be -1, not 0.
	buf := []byte{0, 0, 0, 0} // 4 bytes of throttle, then EOF
	r := bufio.NewReader(bytes.NewReader(buf))
	_, _, prr, _, err := readFetchResponseHeaderV11(r, len(buf))
	if err == nil {
		t.Fatalf("expected an error decoding truncated response, got nil")
	}
	if !strings.Contains(err.Error(), "") || prr != -1 {
		t.Fatalf("preferredReadReplica default: got %d, want -1", prr)
	}
}
