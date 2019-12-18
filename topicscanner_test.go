package kafka

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestTopicScanner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scenario string
		function func(*testing.T, context.Context, *topicScanner)
	}{
		{
			scenario: "calling topic scanner subscribe and getting back the topics that match regex",
			function: testScannerSubscribe,
		},
		{
			scenario: "calling topic scanner unsubscribe and successfully un-subscribing",
			function: testScannerUnsubscribe,
		},
	}
	err := os.Setenv("TOPIC_SCANNER_INTERVAL_MS", "1000")
	if err != nil {
		t.Fatalf("could not scanner interval: %v", err)
	}

	for _, test := range tests {
		testFunc := test.function
		t.Run(test.scenario, func(t *testing.T) {

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			scanner := getTopicScanner()
			testFunc(t, ctx, scanner)
			defer scanner.close()

		})
	}
}

func testScannerSubscribe(t *testing.T, ctx context.Context, scanner *topicScanner) {
	topic1 := makeTopic()
	topic2 := makeTopic()
	topic3 := makeTopic()
	createTopic(t, topic1, 1)
	createTopic(t, topic2, 2)
	createTopic(t, topic3, 3)

	regex := topic1 + "|" + topic2
	brokers := []string{"localhost:9092"}
	_, updateChan, _, err := scanner.subscribe(regex, brokers)
	if err != nil {
		t.Fatalf("could not subscribe to scanner: %v", err)
	}
outer:
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("updated topics did not come in time")

		case partitionsByTopic := <-updateChan:
			if len(partitionsByTopic) != 2 {
				t.Fatalf("unexpected number of topics recieved by scanner: %v", partitionsByTopic)
			}
			if partitions, ok := partitionsByTopic[topic1]; !ok {
				t.Fatalf("topic1 not returned by scanner: %v", partitionsByTopic)
			} else {
				if len(partitions) != 1 {
					t.Fatalf("topic1 returned by scanner does not have only 1 partition listed: %v", partitionsByTopic)
				}
			}
			if partitions, ok := partitionsByTopic[topic2]; !ok {
				t.Fatalf("topic2 not returned by scanner: %v", partitionsByTopic)
			} else {
				if len(partitions) != 2 {
					t.Fatalf("topic2 returned by scanner does not have only 2 partition listed: %v", partitionsByTopic)
				}
			}
			break outer
		}
	}

}

func testScannerUnsubscribe(t *testing.T, ctx context.Context, scanner *topicScanner) {
	topic1 := makeTopic()
	topic2 := makeTopic()
	createTopic(t, topic1, 1)
	createTopic(t, topic2, 2)

	regex := topic1 + "|" + topic2
	brokers := []string{"localhost:9092"}

	subscriberID, updatechan, unsubscribeChan, err := scanner.subscribe(regex, brokers)
	if err != nil {
		t.Fatalf("could not subscribe to scanner: %v", err)
	}
	_, updatechan2, _, err := scanner.subscribe(regex, brokers)
	if err != nil {
		t.Fatalf("could not subscribe to scanner: %v", err)
	}

	//unsubscribe the second subscriber, make sure only one ise left

outer:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("scanner subscribers did not decrease in time")
		case _ = <-updatechan:

			//close this channel after getting the first message
			if len(scanner.subscribers) == 1 {
				break outer
			}
			unsubscribeChan <- subscriberID
		case <-updatechan2:

		}

	}
}
