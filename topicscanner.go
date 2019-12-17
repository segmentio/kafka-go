package kafka

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
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
		updateChan chan map[string][]int
	}
)

//Singleton getter for the topic scanner
//All processes that want to subscribe to the scanner should call this first
//The first process to call this method will instantiate the topic scanner, and get it to start scanning
//TOPIC_SCANNER_INTERVAL_MS is the environment variable that determines how often the scanner should wake up to scan
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
		topicscanner.startScanning()
	})
	return topicscanner
}

//This is the main loop for the topic scanner
//It handles:
// 1) instructions to close, calling cleanup()
// 2) clients un-subscribing from the topic scanner
// 3) updating each subscriber with the current list of topics that match their regex pattern
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

				t.updateSubscribers()
				time.Sleep(time.Duration(t.scanIntervalMS) * time.Millisecond)
			}
		}

	}()
}

//This is the way in which subscribers to the topic scanner formally subscribe to the scanner
//It returns back:
// 1) the id of the subscription
// 2) the channel that the scanner will update the client on it's current topics
// 3) the channel that the subscriber should use to inform the scanner that it should no longer send it updates
func (t *topicScanner) subscribe(regex string, brokers []string) (subscriberID string, updateChannel chan map[string][]int, unsubscribeChannel chan string, err error) {

	if len(regex) == 0 {
		err = errors.New("regex must be non-empty in order to subscribe")
		return
	}
	if len(brokers) == 0 {
		err = errors.New("brokers must be non-empty in order to subscribe")
		return
	}
	updateChannel = make(chan map[string][]int, 1)
	unsubscribeChannel = t.unsubscribeChan
	subscriber := topicScannerSubscriber{
		updateChan: updateChannel,
		regex:      regex,
		brokers:    brokers,
	}
	subscriber.generateID()
	t.subscribers = append(t.subscribers, subscriber)

	subscriberID = subscriber.id
	go func() {
		updateChannel <- t.getSubscriberTopics(subscriber, map[string]map[string][]int{})
	}()

	return
}

//Should not be called directly, if you want to unsubscribe, send a message on the unsubscribe channel
//Remove the subscription as to no longer update it during the recurring scans
func unsubscribe(subscriberID string) {
	for index, subscriber := range topicscanner.subscribers {
		if subscriber.id == subscriberID {
			close(subscriber.updateChan)
			topicscanner.subscribers = append(topicscanner.subscribers[index:], topicscanner.subscribers[:index+1]...)
		}
	}
}

//Run through all the subscribers
func (t *topicScanner) updateSubscribers() {
	brokerTopics := make(map[string]map[string][]int)
	for _, subscriber := range t.subscribers {
		subscriberTopics := t.getSubscriberTopics(subscriber, brokerTopics)
		subscriber.updateChan <- subscriberTopics
	}

}

//Get the specific topics that match the subscribers' regex
//This mutates the brokerTopics map that it gets passed in, in order to keep track of
//all the topics/partitions in each broker to save the scanner from scanning the same
//broker multiple times for multiple subscribers that have it
func (t *topicScanner) getSubscriberTopics(subscriber topicScannerSubscriber, brokerTopics map[string]map[string][]int) (subscriberTopics map[string][]int) {
	subscriberTopics = map[string][]int{}

	//cant compile regex, no point to continue
	rgx, err := regexp.Compile(subscriber.regex)
	if err != nil {
		return
	}
	for _, broker := range subscriber.brokers {
		topics, ok := brokerTopics[broker]
		if !ok {
			conn, err := Dial("tcp", broker)
			if err != nil {
				continue
			}
			topics, err := conn.ListTopics()
			if err != nil {
				continue
			}
			conn.Close()

			if len(topics) > 0 {
				brokerTopics[broker] = topics
				for topic, newPartitions := range topics {
					if rgx.MatchString(topic) {
						if _, ok := subscriberTopics[topic]; ok {
							subscriberTopics[topic] = append(subscriberTopics[topic], newPartitions...)
						} else {
							subscriberTopics[topic] = newPartitions
						}
					}
				}
			}

		} else {
			for topic, newPartitions := range topics {
				if rgx.MatchString(topic) {
					if _, ok := subscriberTopics[topic]; ok {
						subscriberTopics[topic] = append(subscriberTopics[topic], newPartitions...)
					} else {
						subscriberTopics[topic] = newPartitions
					}
				}
			}
		}
		brokerTopics[broker] = subscriberTopics
	}

	return
}

//Instruct the topic scanner to close
//Will trigger cleanup()
func (t *topicScanner) close() {
	t.closeChan <- struct{}{}
}

//The Cleanup process for the topic scanner
//Close all of the subscribers' topics
//Close the close channel
//Clear the subscribers
func (t *topicScanner) cleanup() {
	for _, subscriber := range t.subscribers {
		close(subscriber.updateChan)
	}
	close(topicscanner.closeChan)
	topicscanner.subscribers = []topicScannerSubscriber{}
}

//generate a unique ID for each subscriber of the topic scanner
func (s *topicScannerSubscriber) generateID() {
	id := s.regex
	for _, broker := range s.brokers {
		id += "_" + broker
	}

	id = fmt.Sprintf(id+"_%016x", rand.Int63())
	s.id = id
}
