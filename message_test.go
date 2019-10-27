package kafka

import (
	"bufio"
	"math/rand"
	"testing"
	"time"
)

func TestMessageSetReaderEmpty(t *testing.T) {
	m := messageSetReader{empty: true}

	noop := func(*bufio.Reader, int, int) (int, error) {
		return 0, nil
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

func TestMessageSize(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 20; i++ {
		t.Run("Run", func(t *testing.T) {
			msg := Message{
				Key:   make([]byte, rand.Intn(200)),
				Value: make([]byte, rand.Intn(200)),
				Time:  randate(),
			}
			expSize := msg.message(nil).size()
			gotSize := msg.size()
			if expSize != gotSize {
				t.Errorf("Expected size %d, but got size %d", expSize, gotSize)
			}
		})
	}

}

// https://stackoverflow.com/questions/43495745/how-to-generate-random-date-in-go-lang/43497333#43497333
func randate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}
