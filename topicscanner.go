package kafka

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	topicscanner              *topicScanner
	once                      sync.Once
	defaultScanningIntervalMS = 10000
)

type (
	topicScanner struct {
		subscribers     []topicScannerSubscriber
		closeChan       chan struct{}
		unsubscribeChan chan string
		scanIntervalMS  int
	}
	topicScannerSubscriber struct {
		id         string
		regex      string
		brokers    []string
		updateChan chan []Partition
	}
)

func (t *topicScanner) startScanning() {

	go func() {
	outer:
		for {
			select {
			case _ = <-t.closeChan:
				t.cleanup()
				break outer
			case subscriberID := <-t.unsubscribeChan:
				unsubscribe(subscriberID)
			default:
				t.updateTopicSubscribers()
				time.Sleep(time.Duration(t.scanIntervalMS) * time.Millisecond)
			}
		}

	}()
}

func (t *topicScanner) subscribe(regex string, brokers []string) (subscriberID string, updateChannel chan []Partition, unsubscribeChannel chan string, err error) {

	if len(regex) == 0 {
		err = errors.New("regex must be non-empty in order to subscribe")
		return
	}
	if len(brokers) == 0 {
		err = errors.New("brokers must be non-empty in order to subscribe")
		return
	}
	updateChannel = make(chan []Partition)
	unsubscribeChannel = t.unsubscribeChan
	subscriber := topicScannerSubscriber{
		updateChan: updateChannel,
		regex:      regex,
		brokers:    brokers,
	}
	subscriber.generateID()
	t.subscribers = append(t.subscribers, subscriber)

	subscriberID = subscriber.id

	return
}

func unsubscribe(subscriberID string) {
	for index, subscriber := range topicscanner.subscribers {
		if subscriber.id == subscriberID {
			close(subscriber.updateChan)
			topicscanner.subscribers = append(topicscanner.subscribers[index:], topicscanner.subscribers[:index+1]...)
		}
	}
}

func (t *topicScanner) updateTopicSubscribers() {
	brokerTopics := make(map[string][]Partition)
	for _, subscriber := range t.subscribers {
		subscriberPartitions := []Partition{}
		for _, broker := range subscriber.brokers {
			b, ok := brokerTopics[broker]
			if !ok {
				conn, err := Dial("tcp", broker)
				if err != nil {
					continue
				}
				partitions, err := conn.ListTopics(subscriber.regex)
				if err == nil && len(partitions) > 0 {
					brokerTopics[broker] = partitions
					subscriberPartitions = append(subscriberPartitions, partitions...)
				}
				conn.Close()

			} else {
				subscriberPartitions = append(subscriberPartitions, b...)
			}
		}
		subscriber.updateChan <- subscriberPartitions
	}

}

func getTopicScanner() *topicScanner {
	once.Do(func() {
		interval := defaultScanningIntervalMS
		intervalString, ok := os.LookupEnv("TOPIC_SCANNER_INTERVAL_MS")
		if ok {
			intervalFromString, err := strconv.Atoi(intervalString)
			if err == nil {
				interval = intervalFromString
			}
		}
		topicscanner = &topicScanner{
			scanIntervalMS:  interval,
			subscribers:     []topicScannerSubscriber{},
			closeChan:       make(chan struct{}),
			unsubscribeChan: make(chan string),
		}

	})
	return topicscanner
}

func (t *topicScanner) close() {
	t.closeChan <- struct{}{}
}

func (t *topicScanner) cleanup() {
	for _, subscriber := range t.subscribers {
		close(subscriber.updateChan)
	}
	close(topicscanner.closeChan)
	topicscanner.subscribers = []topicScannerSubscriber{}
}

func (s *topicScannerSubscriber) generateID() {
	id := s.regex
	for _, broker := range s.brokers {
		id += "_" + broker
	}
	s.id = id
}
