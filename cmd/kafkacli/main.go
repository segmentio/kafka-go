package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"

	"github.com/pkg/errors"
	"github.com/segmentio/conf"
	"github.com/segmentio/events"
	_ "github.com/segmentio/events/ecslogs"
	_ "github.com/segmentio/events/log"
	_ "github.com/segmentio/events/sigevents"
	_ "github.com/segmentio/events/text"
	kafka "github.com/segmentio/kafka-go"
)

var version = ""

func main() {
	var err error
	var ld = conf.Loader{
		Name: "kafkacli",
		Args: os.Args[1:],
		Commands: []conf.Command{
			{"tail", "Tail a partition or topic"},
			{"help", "Show the kafkacli help"},
			{"version", "Show the kafkacli version"},
		},
	}

	switch cmd, args := conf.LoadWith(nil, ld); cmd {
	case "tail":
		err = tail(args)
	case "help":
		ld.PrintHelp(nil)
	case "version":
		fmt.Println(version)
	default:
		panic("unreachable")
	}

	if err != nil {
		events.Log("%{error}s", err)
		os.Exit(1)
	}
}

func tail(args []string) (err error) {
	config := struct {
		Debug     bool   `conf:"debug"`
		ConsulURL string `conf:"consul-url"`
		KafkaURL  string `conf:"kafka-url"`
	}{
		Debug:     false,
		ConsulURL: "http://localhost:8500",
		KafkaURL:  "kafka://localhost:9092/events",
	}

	conf.LoadWith(&config, conf.Loader{
		Name: "kafkacli tail",
		Args: args,
	})

	events.DefaultLogger.EnableDebug = config.Debug
	events.DefaultLogger.EnableSource = config.Debug

	defer func() {
		if v := recover(); v != nil {
			err = convertPanicToError(v)
		}
	}()

	url, err := url.Parse(config.KafkaURL)
	if err != nil {
		return errors.Wrap(err, "failed to parse kafka url")
	}

	if url.Scheme != "kafka" {
		return errors.New("invalid kafka url scheme")
	}

	broker := url.Host
	topic := url.Path[1:len(url.Path)]

	fmt.Printf("topic: %s\n", topic)
	fmt.Printf("broker: %s\n", broker)

	reader, err := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
	})

	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	offset, err := reader.Seek(ctx, kafka.OffsetOldest)
	if err != nil {
		return
	}

	fmt.Printf("starting at offset: %d\n", offset)
	fmt.Println("")

	for {
		msg, err := reader.Read(ctx)
		if err != nil {
			fmt.Printf("error reading message: %s\n", err.Error())
			continue
		}

		fmt.Printf("read message at offset %d:\n\t%v\n\n", msg.Offset, string(msg.Value))
	}

	return reader.Close()
}

func signals(signals ...os.Signal) (<-chan os.Signal, func()) {
	sigchan := make(chan os.Signal)
	sigrecv := events.Signal(sigchan)
	signal.Notify(sigchan, signals...)
	return sigrecv, func() { signal.Stop(sigchan) }
}

func convertPanicToError(v interface{}) error {
	switch x := v.(type) {
	case error:
		return x
	case string:
		return errors.New(x)
	default:
		return fmt.Errorf("%v", x)
	}
}
