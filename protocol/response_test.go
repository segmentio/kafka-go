package protocol

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadResponseUnexpectedTLSDetection(t *testing.T) {
	var buf bytes.Buffer

	buf.Write([]byte{tlsAlertByte, 0x03, 0x03, 10, 0, 0, 0})

	correlationID, _, err := ReadResponse(&buf, ApiVersions, 0)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected an io.ErrUnexpectedEOF from ReadResponse got %v", err)
	}

	if !strings.Contains(err.Error(), "broker appears to be expecting TLS") {
		t.Fatalf("expected error messae to contain %s got %s", "broker appears to be expecting TLS", err.Error())
	}

	if correlationID != 0 {
		t.Fatalf("expected correlationID of 0 got %d", correlationID)
	}
}
