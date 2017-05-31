package kafka

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"testing"
)

func TestMessageCRC32(t *testing.T) {
	t.Parallel()

	m := message{
		MagicByte: 1,
		Timestamp: 42,
		Key:       nil,
		Value:     []byte("Hello World!"),
	}

	b := &bytes.Buffer{}
	w := bufio.NewWriter(b)
	write(w, m)
	w.Flush()

	h := crc32.NewIEEE()
	h.Write(b.Bytes()[4:])

	sum1 := h.Sum32()
	sum2 := uint32(m.crc32())

	if sum1 != sum2 {
		t.Error("bad CRC32:")
		t.Logf("expected: %d", sum1)
		t.Logf("found:    %d", sum2)
	}
}
