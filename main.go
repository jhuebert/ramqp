package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func handler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	bodyString := string(body)
	log.Info(bodyString)

	log.Printf("declared Exchange, publishing %dB body (%q)", len(body), body)
	if err = channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}

}

func main() {

	uri := flag.String("uri", "", "AMQP URI")
	reliable := flag.Bool("reliable", true, "Wait for publisher confirmation")
	apiKeyPath := flag.String("apiKeyPath", "", "Path to file containing allowed API keys")
	logLevel := flag.String("log", "info", "logging level. Can be one of [trace, debug, info, warn, error, fatal, panic]")
	flag.Parse()

	level, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Warnf("Invalid input log level - %q", *logLevel)
		log.Warnf("Setting log level to info")
		level = log.InfoLevel
	}
	log.SetLevel(level)

	if *apiKeyPath == "" {
		log.Error("API key file is required")
		flag.Usage()
		return
	} else if !fileExists(*apiKeyPath) {
		log.Errorf("API key file does not exist - %q", *apiKeyPath)
		flag.Usage()
		return
	}

	if *uri == "" {
		log.Error("AMQP URI must be set")
		flag.Usage()
		return
	}

	log.Printf("dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	if reliable {
		log.Printf("enabling publishing confirms.")
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms) //TODO Needs to be done for each publish
	}

	//route /amqp/{exchange}?routingKey=&mandatory=false&immediate=false&deliveryMode=persistent&priority=0
	//Content type is copied
	//Body of HTTP request is body of AMQP
	//header API-KEY X
	//allow whitelisting and blacklisting of API keys
	//have POST for publish
	//have GET for polling queue for message
	//have publish API keys separate from consuming API keys
	//GET would necessarily auto-ack

	// DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
	// Priority:        0,              // 0-9
	r := mux.NewRouter()
	r.HandleFunc("/ampqp/{exchange}", handler).Methods("POST")

	srv := &http.Server{
		Handler:      r,
		Addr:         ":8080",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
