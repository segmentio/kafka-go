package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
)

func TestIsse477(t *testing.T) {
	// This test verifies that a connection attempt with a minimal TLS
	// configuration does not panic.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	cg := connGroup{
		addr: l.Addr(),
		pool: &connPool{
			dial: defaultDialer.DialContext,
			tls:  &tls.Config{},
		},
	}

	if _, err := cg.connect(context.Background()); err != nil {
		// An error is expected here because we are not actually establishing
		// a TLS connection to a kafka broker.
		t.Log(err)
	} else {
		t.Error("no error was reported when attempting to establish a TLS connection to a non-TLS endpoint")
	}
}
