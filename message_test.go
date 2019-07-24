package kafka

import (
	"testing"
)

func TestMessageSetReaderEmpty(t *testing.T) {
	m := messageSetReader{}

	noop := func(rb *readBuffer, n int) {
		rb.discard(n)
	}

	offset, timestamp, headers, err := m.readMessage(0, noop, noop)
	if offset != 0 {
		t.Errorf("expected offset of 0, get %d", offset)
	}

	if timestamp != 0 {
		t.Errorf("expected timestamp of 0, get %d", timestamp)
	}

	if headers != nil {
		t.Errorf("expected nil headers, got %v", headers)
	}

	if err != RequestTimedOut {
		t.Errorf("expected RequestTimedOut, got %v", err)
	}

	if m.remaining() != 0 {
		t.Errorf("expected 0 remaining, got %d", m.remaining())
	}

	if m.discard() != nil {
		t.Errorf("unexpected error from discard(): %v", m.discard())
	}
}
