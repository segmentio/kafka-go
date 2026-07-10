package protocol

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"
)

// TestReadFromVersion2NegativeRecordCountReturnsError reproduces a broker
// response whose v2 record batch has a well-formed length and checksum, but
// a negative record count. Before this was rejected, the count fed straight
// into make([]optimizedRecord, numRecords), which panics with "makeslice:
// len out of range" instead of returning a decode error — turning a
// malformed or malicious broker response into a crash.
func TestReadFromVersion2NegativeRecordCountReturnsError(t *testing.T) {
	rs := RecordSet{
		Version: 2,
		Records: NewRecordReader(Record{
			Offset: 0,
			Time:   time.Unix(0, 0),
			Key:    NewBytes([]byte("key")),
			Value:  NewBytes([]byte("value")),
		}),
	}

	buf := &bytes.Buffer{}
	if _, err := rs.WriteTo(buf); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()

	// Layout written by WriteTo+writeToVersion2: a 4-byte outer size field,
	// followed by the v2 batch header. Within that header (see the offset
	// comments in writeToVersion2), the CRC32 field is at +17 and covers
	// everything from +21 onward, and numRecords is at +57 — so relative to
	// the start of data, that's +21, +25, and +61 respectively.
	const (
		crcOffset        = 4 + 17
		crcCoverStart    = 4 + 21
		numRecordsOffset = 4 + 57
	)

	binary.BigEndian.PutUint32(data[numRecordsOffset:], ^uint32(0)) // -1 as int32

	checksum := crc32.Checksum(data[crcCoverStart:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(data[crcOffset:], checksum)

	var out RecordSet
	_, err := out.ReadFrom(bytes.NewReader(data))
	if err == nil {
		t.Fatal("expected an error decoding a record batch with a negative record count, got nil")
	}
}
