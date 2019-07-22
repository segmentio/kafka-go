package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestSASLHandshakeRequestV0(t *testing.T) {
	item := saslHandshakeRequestV0{
		Mechanism: "SCRAM-SHA-512",
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found saslHandshakeRequestV0
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

func TestSASLHandshakeResponseV0(t *testing.T) {
	item := saslHandshakeResponseV0{
		ErrorCode:         2,
		EnabledMechanisms: []string{"PLAIN", "SCRAM-SHA-512"},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found saslHandshakeResponseV0
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
