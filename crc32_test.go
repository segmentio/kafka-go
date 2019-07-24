package kafka

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func TestMessageCRC32(t *testing.T) {
	m := message{
		MagicByte: 1,
		Timestamp: 42,
		Key:       nil,
		Value:     []byte("Hello World!"),
	}

	b := &bytes.Buffer{}
	w := &writeBuffer{w: b}
	w.write(m)

	h := crc32.New(crc32.IEEETable)
	h.Write(b.Bytes()[4:])

	sum1 := h.Sum32()
	sum2 := uint32(m.crc32(&crc32Writer{table: crc32.IEEETable}))

	if sum1 != sum2 {
		t.Error("bad CRC32:")
		t.Logf("expected: %d", sum1)
		t.Logf("found:    %d", sum2)
	}
}
