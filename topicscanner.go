package kafka

import (
	"context"
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
		subscribers          []topicScannerSubscriber
		updateSubscriberChan chan topicScannerSubscriber
		closeChan            chan struct{}
		unsubscribeChan      chan string
		done                 chan struct{}
		scanIntervalMS       int
		closed               bool
		mutex                sync.Mutex
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
	updateSubscriberChan := make(chan topicScannerSubscriber)
	closeChan := make(chan struct{}, 1)
	unsubscribeChan := make(chan string)
	doneChan := make(chan struct{}, 1)
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
			scanIntervalMS:       interval,
			subscribers:          []topicScannerSubscriber{},
			updateSubscriberChan: updateSubscriberChan,
			closeChan:            closeChan,
			unsubscribeChan:      unsubscribeChan,
			done:                 doneChan,
			closed:               false,
		}
		topicscanner.startScanning()
	})

	topicscanner.mutex.Lock()
	if topicscanner.closed {

		topicscanner.updateSubscriberChan = updateSubscriberChan
		topicscanner.unsubscribeChan = unsubscribeChan
		topicscanner.closeChan = closeChan
		topicscanner.done = doneChan
		topicscanner.closed = false
		topicscanner.startScanning()
	}
	topicscanner.mutex.Unlock()

	return topicscanner
}

//This is the main loop for the topic scanner
//It handles:
// 1) instructions to close, calling cleanup()
// 2) clients un-subscribing from the topic scanner
// 3) updating each subscriber with the current list of topics that match their regex pattern
func (t *topicScanner) startScanning() {

	go func() {
		ticker := time.NewTicker(time.Duration(t.scanIntervalMS) * time.Millisecond)
	outer:
		for {
			select {
			case _ = <-t.closeChan:
				t.cleanup()
				t.done <- struct{}{}
				break outer
			case subscriberID := <-t.unsubscribeChan:

				unsubscribe(subscriberID)
			case subscriber := <-t.updateSubscriberChan:
				subscriberTopics := t.getSubscriberTopics(subscriber, make(map[string]map[string][]int))
				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				select {
				case <-ctx.Done():
					break
				case subscriber.updateChan <- subscriberTopics:

				}
			case _ = <-ticker.C:
				t.updateSubscribers()
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
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		select {
		case <-ctx.Done():
		case t.updateSubscriberChan <- subscriber:
		}
	}()

	return
}

//Should not be called directly, if you want to unsubscribe, send a message on the unsubscribe channel
//Remove the subscription as to no longer update it during the recurring scans
func unsubscribe(subscriberID string) {

	for index, subscriber := range topicscanner.subscribers {
		if subscriber.id == subscriberID {
			close(subscriber.updateChan)
			topicscanner.subscribers = append(topicscanner.subscribers[:index], topicscanner.subscribers[index+1:]...)
			break
		}
	}
}

//Run through all the subscribers
func (t *topicScanner) updateSubscribers() {

	brokerTopics := make(map[string]map[string][]int)
	wg := sync.WaitGroup{}
	wg.Add(len(topicscanner.subscribers))
	for _, subscriber := range t.subscribers {
		subscriberTopics := t.getSubscriberTopics(subscriber, brokerTopics)

		//dont block if the thing reading from this isn't reading from it yet
		//stop trying to send after timeout, next iteration will get it
		go func() {
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			select {
			case <-ctx.Done():
				break
			case subscriber.updateChan <- subscriberTopics:

			}
			wg.Done()
		}()
	}
	wg.Wait()
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
func (t *topicScanner) close() error {
	t.closeChan <- struct{}{}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	select {
	case <-ctx.Done():
		return errors.New("could not close scanner in time")
	case <-t.done:
		close(t.done)
		t.closed = true
		break
	}
	return nil
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
	close(topicscanner.updateSubscriberChan)
	close(topicscanner.unsubscribeChan)

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
