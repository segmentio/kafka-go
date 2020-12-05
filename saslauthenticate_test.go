package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSASLAuthenticateRequestV1(t *testing.T) {
	item := saslAuthenticateRequestV1{
		Data: []byte("\x00user\x00pass"),
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found saslAuthenticateRequestV1
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
		SessionLifeTimeMs: 1000,
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
