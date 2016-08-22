// this package explores:
//- nsq.consumer
//- max-in-flight

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	nsq "github.com/bitly/go-nsq"
)

// Command line arguments
var (
	topic         = flag.String("topic", "", "NSQ topic to subscribe")
	channel       = flag.String("channel", "", "NSQ channel to fetch message from")
	maxInFlights  = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	totalMessages = flag.Int("n", 0, "total messages to show (will wait if starved)")

	lookupdHTTPAddrs = &StringArray{}
)

func init() {
	flag.Var(lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP addresses (may be given multiple times)")
}

// TailHandler keeps track of received-message count.
// Note: Not thread-safety.
type TailHandler struct {
	totalMessages int
	messagesShown int
}

// HandleMessage implements nsq.ConsumerHandler.
func (th *TailHandler) HandleMessage(m *nsq.Message) error {

	log.Printf("incoming message: %v \n", m)
	th.messagesShown++

	var err error

	if _, err = os.Stdout.Write(m.Body); err != nil {
		log.Fatalf("failed to write to os.Stdout - %s", err)
	}

	if _, err = os.Stdout.WriteString("\n"); err != nil {
		log.Fatalf("failed to write to os.Stdout - %s", err)
	}

	if th.totalMessages > 0 && th.messagesShown >= th.totalMessages {
		os.Exit(0)
	}
	return nil
}

func main() {

	cfg := nsq.NewConfig()

	flag.Parse()

	if *topic == "" {
		log.Fatal("--topic is required.")
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if len(*lookupdHTTPAddrs) == 0 {
		log.Fatal("--lookupd-http-addrress is required.")
	}

	if *maxInFlights < 0 {
		log.Fatal("--max-in-flight must be greater than 0.")
	}

	if *totalMessages < 0 {
		log.Fatal("--n must be greater than 0.")
	}

	// don't ask for more messages than we want.
	if *totalMessages < *maxInFlights {
		*maxInFlights = *totalMessages
	}

	// nsq-consumer setup
	cfg.UserAgent = fmt.Sprintf("go-nsq/%s", nsq.VERSION)
	cfg.MaxInFlight = *maxInFlights

	var consumer *nsq.Consumer
	var err error

	if consumer, err = nsq.NewConsumer(*topic, *channel, cfg); err != nil {
		log.Fatal(err)
	}
	consumer.AddHandler(&TailHandler{totalMessages: *totalMessages})

	// nsq-consumer initiate connection
	if err = consumer.ConnectToNSQLookupds(*lookupdHTTPAddrs); err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// i/o loop - multiplexing stop/terminate signal
	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			// user sigs to close nsq.consumer, gracefully.
			consumer.Stop()
		}
	}
}

// StringArray implements flag.Value interface.
type StringArray []string

// Set appends s to slice
func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

// Set returns slice elements as string
func (a *StringArray) String() string {
	return strings.Join(*a, ", ")
}
