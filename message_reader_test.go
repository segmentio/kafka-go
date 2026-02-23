package kafka

import (
	"bufio"
	"bytes"
	"testing"
)

func TestMarkReadDoesNotPanicOnZeroCount(t *testing.T) {
	// On compacted topics, the record count in a batch header may exceed
	// the number of records actually present. markRead must not panic
	// when count has already reached zero.
	r := &messageSetReader{
		readerStack: &readerStack{
			reader: bufio.NewReader(bytes.NewReader(nil)),
			count:  0,
		},
	}
	// This previously caused: panic("markRead: negative count")
	r.markRead()
	if r.count != 0 {
		t.Fatalf("expected count to remain 0, got %d", r.count)
	}
}

func TestMarkReadDecrementsCount(t *testing.T) {
	r := &messageSetReader{
		readerStack: &readerStack{
			reader: bufio.NewReader(bytes.NewReader(nil)),
			count:  3,
		},
	}
	r.markRead()
	if r.count != 2 {
		t.Fatalf("expected count=2, got %d", r.count)
	}
}
