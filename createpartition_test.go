package kafka

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"testing"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestCreatePartitionsResponseV0(t *testing.T) {
	item := createPartitionsResponseV0{
		ThrottleTimeMs: 100,
		Results: []createPartitionsResponseV0Result{
			{
				Name:      "test",
				ErrorCode: 0,
			},
		},
	}

	b := bytes.NewBuffer(nil)
	w := &writeBuffer{w: b}
	item.writeTo(w)

	var found createPartitionsResponseV0
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

func TestCreatePartitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario   string
		minVersion string
		function   func(*testing.T)
	}{

		{
			scenario:   "create partitions",
			minVersion: "1.0.1",
			function:   testCreatePartitions,
		},
	}

	for _, test := range tests {
		if !ktesting.KafkaIsAtLeast(test.minVersion) {
			t.Log("skipping " + test.scenario + " because broker is not at least version " + test.minVersion)
			continue
		}
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()
			testFunc(t)
		})
	}
}
func testCreatePartitions(t *testing.T) {
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
	brokers, err := conn.Brokers()
	if err != nil {
		return
	}
	brokerIds := []int32{}
	for _, b := range brokers {
		brokerIds = append(brokerIds, int32(b.ID))
	}

	err = conncontroller.CreatePartitions(CreatePartitionsConfig{
		Topics: []Topic{{
			Name:        topic,
			Count:       2,
			Assignments: [][]int32{brokerIds},
		},
		},
		TimeoutMs:    20,
		ValidateOnly: false,
	})
	if err != nil {
		fmt.Println(err)
	}
}
