package kafka

import (
	"bufio"
	"bytes"
	"net"
	"reflect"
	"strconv"
	"testing"
)

func TestAlterConfigsResponseV0(t *testing.T) {
	item := alterConfigsResponseV0{
		ThrottleTimeMs: 100,
		Resources: []alterConfigsResponseV0Resource{
			{
				ErrorCode:    0,
				ResourceType: int8(2),
				ResourceName: "testTopic",
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found alterConfigsResponseV0
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

func TestAlterConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T)
	}{
		{
			scenario: "alter configs",
			function: testAlterConfigs,
		},
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()
			testFunc(t)
		})
	}
}

func testAlterConfigs(t *testing.T) {
	const topic = "test-1"
	createTopic(t, topic, 1)
	conn, err := Dial("tcp", "localhost:9092")
	if err != nil {
		return
	}
	controller, _ := conn.Controller()
	conncontroller, err := Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return
	}

	err = conncontroller.AlterConfigs(AlterConfig{
		Resources: []Resource{
			{
				ResourceType: int8(2),
				ResourceName: topic,
				ConfigEntries: []ConfigEntry{{
					ConfigName:  "max.message.bytes",
					ConfigValue: "200000",
				},
				},
			},
		},
		ValidateOnly: false})

	if err != nil {
		return
	}
}
