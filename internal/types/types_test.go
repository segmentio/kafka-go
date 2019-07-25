package types

import (
	"bytes"
	"reflect"
	"sort"
	"testing"
)

var testValues = []interface{}{
	&RequestHeader{
		APIKey:        1,
		APIVersion:    2,
		CorrelationID: 3,
		ClientID:      "Luke",
	},
}

func addTestValues(values ...interface{}) {
	testValues = append(testValues, values...)
}

func sortTestValues() {
	sort.Slice(testValues, func(i, j int) bool {
		t1 := reflect.TypeOf(testValues[i]).Elem()
		t2 := reflect.TypeOf(testValues[j]).Elem()
		return t1.String() < t2.String()
	})
}

func TestMarshalUnmarshal(t *testing.T) {
	sortTestValues()

	for _, v := range testValues {
		t.Run(reflect.TypeOf(v).Elem().String(), func(t *testing.T) {
			testMarshalUnmarshal(t, v)
		})
	}
}

func testMarshalUnmarshal(t *testing.T, v interface{}) {
	b := &bytes.Buffer{}
	w := reflect.New(reflect.TypeOf(v).Elem()).Interface()

	if err := Marshal(b, v); err != nil {
		t.Fatal("marshal:", err)
	}

	if err := Unmarshal(b, w); err != nil {
		t.Fatal("unmarshal:", err)
	}

	if !reflect.DeepEqual(v, w) {
		t.Error("values don't match")
		t.Logf("expected: %+v", v)
		t.Logf("found:    %+v", w)
	}
}
