package protocol

import (
	"bytes"
	"io"
	"testing"
)

// Regression tests for https://github.com/segmentio/kafka-go/issues/1397
// ("Panic when write batchs with async").
//
// encoder.Write evaluates e.update(b[:n]) with the count reported by the
// underlying writer. A writer that over-reports its byte count makes n exceed
// cap(b), which panics with "slice bounds out of range" instead of failing
// cleanly. RecordSet.WriteTo then discarded both the error and the short count
// from its final flush, so a truncated batch was reported as a complete write
// and the caller lost records silently.

// overReportingWriter claims more bytes than it was given. Illegal for
// io.Writer, and the precondition that produced the reported panic.
type overReportingWriter struct {
	sink bytes.Buffer
	lie  int
}

func (w *overReportingWriter) Write(b []byte) (int, error) {
	n, err := w.sink.Write(b)
	if err != nil {
		return n, err
	}
	return n + w.lie, nil
}

// shortWriter accepts fewer bytes than it was given without reporting an error,
// which is legal for io.Writer and is what a writer under backpressure does.
type shortWriter struct {
	sink bytes.Buffer
}

func (w *shortWriter) Write(b []byte) (int, error) {
	if len(b) > 35 {
		b = b[:35]
	}
	return w.sink.Write(b)
}

func testRecordSet() *RecordSet {
	return &RecordSet{
		Version: 2,
		Records: NewRecordReader(Record{
			Key:   NewBytes([]byte("k")),
			Value: NewBytes(bytes.Repeat([]byte("v"), 640)),
		}),
	}
}

// encoder.Write must reject a byte count larger than the slice it was handed,
// rather than evaluating b[:n] and panicking.
func TestEncoderWriteRejectsOverReportedCount(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 640)
	r := bytes.NewReader(payload)

	w := &overReportingWriter{lie: 35}
	e := &encoder{}
	e.Reset(w)

	n, err := io.Copy(e, r)
	if err == nil {
		t.Error("expected an error from an over-reporting writer, got nil")
	}
	if n != int64(len(payload)) {
		t.Errorf("expected %d bytes copied, got %d", len(payload), n)
	}
}

// A batch flushed into a writer that accepts only part of it must surface a
// short-write error instead of reporting success.
func TestRecordSetWriteToSurfacesShortWrite(t *testing.T) {
	w := &shortWriter{}

	n, err := testRecordSet().WriteTo(w)
	if err != io.ErrShortWrite {
		t.Errorf("expected io.ErrShortWrite, got %v", err)
	}
	if n >= 640 {
		t.Errorf("expected fewer than 640 bytes reported as written, got %d", n)
	}
}

// A writer that accepts everything must still succeed unchanged.
func TestRecordSetWriteToFullWriteSucceeds(t *testing.T) {
	w := &bytes.Buffer{}

	n, err := testRecordSet().WriteTo(w)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n <= 0 {
		t.Errorf("expected a positive byte count, got %d", n)
	}
	if int(n) != w.Len() {
		t.Errorf("reported %d bytes written but buffer holds %d", n, w.Len())
	}
}
