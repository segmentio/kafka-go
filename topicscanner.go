package kafka

import (
	"context"
	"regexp"
	"sync"
	"time"
)

var (
	defaultScanningIntervalMS = 10000
)

type (
	TopicScannerConfig struct {
		Brokers            []string
		TopicRegex         string
		ScanningIntervalMS int
	}

	TopicScanner struct {
		config             TopicScannerConfig
		currentFoundTopics *[]string
		closeChan          chan struct{}
		regex              regexp.Regexp
		mutex              sync.Mutex
	}
)

func NewTopicScanner(config TopicScannerConfig) (*TopicScanner, error) {

	rgx, err := regexp.Compile(config.TopicRegex)
	if err != nil {
		return nil, err
	}
	//If the scanning interval isn't set properly, scan every 10 seconds
	if config.ScanningIntervalMS <= 0 {
		config.ScanningIntervalMS = defaultScanningIntervalMS
	}
	return &TopicScanner{
		currentFoundTopics: new([]string),
		config:             config,
		regex:              *rgx,
		closeChan:          make(chan struct{}),
	}, nil
}

func (t *TopicScanner) StartScanning(ctx context.Context) {
	//Initial fetching of the topics
	t.updateTopics(ctx)
	go func() {
		ticker := time.NewTicker(time.Duration(t.config.ScanningIntervalMS) * time.Millisecond)
	outer:
		for {
			select {
			case _ = <-ticker.C:
				t.updateTopics(ctx)
			case _ = <-ctx.Done():
				break outer
			case _ = <-t.closeChan:
				break outer
			}
		}

	}()
}

func (t *TopicScanner) GetTopics() []string {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return *t.currentFoundTopics
}

func (t *TopicScanner) Close() {
	t.closeChan <- struct{}{}
}

func (t *TopicScanner) updateTopics(ctx context.Context) {
	conn, err := t.initialize(ctx)
	if err != nil {
		return
	}
	topics, err := conn.ListTopics()
	if err != nil {
		return
	}
	conn.Close()

	if len(topics) > 0 {
		newTopics := []string{}
		for _, topic := range topics {
			if t.regex.MatchString(topic) {
				newTopics = append(newTopics, topic)
			}
		}
		t.mutex.Lock()
		t.currentFoundTopics = &newTopics
		t.mutex.Unlock()
	}
}

func (t *TopicScanner) initialize(ctx context.Context) (conn *Conn, err error) {
	for i := 0; i != len(t.config.Brokers) && conn == nil; i++ {
		var broker = t.config.Brokers[i]
		conn, err = DialContext(ctx, "tcp", broker)

		if err != nil {
			continue
		}

		conn.SetDeadline(time.Time{})
	}

	return
}
