package kafka

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestDiscardN(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, *bufio.Reader, int)
	}{
		{
			scenario: "discard nothing",
			function: func(t *testing.T, r *bufio.Reader, sz int) {
				remain, err := discardN(r, sz, 0)
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if remain != sz {
					t.Errorf("Expected all bytes remaining, got %d", remain)
				}
			},
		},
		{
			scenario: "discard fewer than available",
			function: func(t *testing.T, r *bufio.Reader, sz int) {
				remain, err := discardN(r, sz, sz-1)
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if remain != 1 {
					t.Errorf("Expected single remaining byte, got %d", remain)
				}
			},
		},
		{
			scenario: "discard all available",
			function: func(t *testing.T, r *bufio.Reader, sz int) {
				remain, err := discardN(r, sz, sz)
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if remain != 0 {
					t.Errorf("Expected no remaining bytes, got %d", remain)
				}
			},
		},
		{
			scenario: "discard more than available",
			function: func(t *testing.T, r *bufio.Reader, sz int) {
				remain, err := discardN(r, sz, sz+1)
				if err != errShortRead {
					t.Errorf("Expected errShortRead, got %v", err)
				}
				if remain != 0 {
					t.Errorf("Expected no remaining bytes, got %d", remain)
				}
			},
		},
		{
			scenario: "discard returns error",
			function: func(t *testing.T, r *bufio.Reader, sz int) {
				remain, err := discardN(r, sz+2, sz+1)
				if err != io.EOF {
					t.Errorf("Expected EOF, got %v", err)
				}
				if remain != 2 {
					t.Errorf("Expected single remaining bytes, got %d", remain)
				}
			},
		},
		{
			scenario: "errShortRead doesn't mask error",
			function: func(t *testing.T, r *bufio.Reader, sz int) {
				remain, err := discardN(r, sz+1, sz+2)
				if err != io.EOF {
					t.Errorf("Expected EOF, got %v", err)
				}
				if remain != 1 {
					t.Errorf("Expected single remaining bytes, got %d", remain)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			msg := []byte("test message")
			r := bufio.NewReader(bytes.NewReader(msg))
			test.function(t, r, len(msg))
		})
	}
}
