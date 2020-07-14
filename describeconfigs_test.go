package kafka

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestDescribeConfigsResponseV0(t *testing.T) {
	item := describeConfigsResponseV0{
		ThrottleTimeMs: 100,
		Resources: []describeConfigsResponseV0Resource{
			{
				ErrorCode:    0,
				ResourceType: int8(2),
				ResourceName: "testTopic",
				ConfigEntries: []describeConfigsResponseV0ConfigEntry{
					{
						ConfigName:  "max.message.bytes",
						ConfigValue: "100000",
						ReadOnly:    false,
						IsDefault:   true,
						IsSensitive: false,
					},
				},
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found describeConfigsResponseV0
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
