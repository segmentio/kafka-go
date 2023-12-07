//go:build go1.21

package kafka_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestSlogCompat(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{AddSource: false})
	slogLogger := slog.New(handler)

	var logger kafka.Logger = kafka.LoggerFunc(slogLogger.Info)
	logger.Printf("test message", "foo", "bar")
	want := `"level":"INFO","msg":"test message","foo":"bar"}`
	if !strings.Contains(buf.String(), want) {
		t.Errorf("expected log line to have suffix %q, got %q", want, buf.String())
	}
}
