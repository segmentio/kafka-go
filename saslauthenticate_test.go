package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSASLAuthenticateRequestV0(t *testing.T) {
	item := saslAuthenticateRequestV0{
		Data: []byte("\x00user\x00pass"),
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found saslAuthenticateRequestV0
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}

func TestSASLAuthenticateResponseV0(t *testing.T) {
	item := saslAuthenticateResponseV0{
		ErrorCode:    2,
		ErrorMessage: "Message",
		Data:         []byte("bytes"),
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found saslAuthenticateResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}

func TestSASLAuthenticateResponseV1(t *testing.T) {
	item := saslAuthenticateResponseV1{
		ErrorCode:         2,
		ErrorMessage:      "Message",
		Data:              []byte("bytes"),
		SessionLifetimeMs: 300000,
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found saslAuthenticateResponseV1
	remain, err := (&found).readFrom(bufio.NewReader(b), b.Len())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if remain != 0 {
		t.Errorf("expected 0 remain, got %v", remain)
		t.FailNow()
	}
	if !reflect.DeepEqual(item, found) {
		t.Error("expected item and found to be the same")
		t.FailNow()
	}
}
