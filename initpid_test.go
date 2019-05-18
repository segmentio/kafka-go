package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestInitProducerIDResponseV0(t *testing.T) {
	pidResponse := initProducerIDResponseV0{
		ThrottleTimeMs: 1,
		ErrorCode:      2,
		ProducerId:     3,
		ProducerEpoch:  4,
	}
	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	pidResponse.writeTo(w)
	w.Flush()

	var found initProducerIDResponseV0
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
	if err != nil {
		t.Fatal("Failed to parse initProducerIDResponseV0", err)
	}
	if remain != 0 {
		t.Fatalf("Exepected 0 remain. Got: %v", remain)
	}
	if !reflect.DeepEqual(pidResponse, found) {
		t.Fatal("expected original item and parsed to be the same")
	}
}
