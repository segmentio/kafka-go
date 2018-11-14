package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestAPIVersionsResponseV1(t *testing.T) {
	item := apiVersionsResponseV1{
		ErrorCode: 2,
		APIVersions: []apiVersionsResponseV1Range{
			{
				APIKey:     1,
				MinVersion: 1,
				MaxVersion: 3,
			},
		},
	}

	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	item.writeTo(w)
	w.Flush()

	var found apiVersionsResponseV1
	remain, err := (&found).readFrom(bufio.NewReader(buf), buf.Len())
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
