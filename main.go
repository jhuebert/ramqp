package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const headerApiKey = "API-KEY"

func main() {

	// Specify all of the available input parameters
	uri := flag.String("uri", "", "AMQP URI. Example - amqp://user:password@localhost:5672/vhost")
	apiKey := flag.String("apiKey", "", "API key that must be included in each request in a header named API-KEY")
	timeout := flag.Uint("timeout", 5, "Read and write timeout in seconds for REST server")
	logLevel := flag.String("log", "info", "Logging level. Can be one of [trace, debug, info, warn, error, fatal, panic]")
	flag.Parse()

	// Verify the log level for the application defaulting to info if none is specified
	level, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Warnf("Invalid input log level: %q", *logLevel)
		log.Warnf("Setting log level to info")
		level = log.InfoLevel
	}
	log.SetLevel(level)

	// Verify that an API key is passed in
	if *apiKey == "" {
		log.Error("API key is required")
		flag.Usage()
		return
	}

	// Verify that a URI is passed in
	if *uri == "" {
		log.Error("AMQP URI is required")
		flag.Usage()
		return
	}

	// Establish a connection to the AMQP server
	connect(uri)

	// Start a new router
	router := mux.NewRouter()

	// Add a POST route that will publish messages to the AMQP server
	handleFunc := router.HandleFunc("/amqp/{exchange}", publishHandler)
	handleFunc.Methods("POST")
	handleFunc.Headers(headerApiKey, *apiKey)

	// Define the REST server
	srv := &http.Server{
		Handler:      router,
		Addr:         ":8080",
		WriteTimeout: time.Duration(*timeout) * time.Second,
		ReadTimeout:  time.Duration(*timeout) * time.Second,
	}

	// Start the REST server
	log.Fatal(srv.ListenAndServe())
}

func publishHandler(w http.ResponseWriter, r *http.Request) {

	// Parse the exchange to publish to
	exchange := mux.Vars(r)["exchange"]
	log.Debugf("exchange: %v", exchange)

	// Parse header values
	apiKey := getHeaderString(r, headerApiKey)
	contentType := getHeaderString(r, "Content-Type")
	contentEncoding := getHeaderString(r, "Content-Encoding")

	// Parse query parameters
	routingKey := getQueryString(r, "routingKey")
	mandatory := getQueryBool(r, "mandatory")
	immediate := getQueryBool(r, "immediate")
	confirm := getQueryBool(r, "confirm")

	// Convert the message priority. Ensure that the priority is in the correct range.
	priority := getQueryInt(r, "priority")
	if (priority < 0) || (priority > 9) {
		log.Warnf("Priority must be 0-9 but was %v. Using 0 instead.", r.FormValue("priority"))
		priority = 0
	}

	// Convert the delivery mode parameter
	deliveryMode := amqp.Transient
	if getQueryString(r, "deliveryMode") == "persistent" {
		deliveryMode = amqp.Persistent
	}

	// Read the body from the request into a byte array
	body, _ := ioutil.ReadAll(r.Body)
	log.Debug("body: %v", string(body))

	// Open an AMQP channel
	channel, err := amqpConnection.Channel()
	if err != nil {
		log.Error(err)
		w.WriteHeader(500)
		return
	}
	defer closeChannel(channel)

	// Add publish confirmation if requested
	var confirmations chan amqp.Confirmation
	if confirm {
		err := channel.Confirm(false)
		if err == nil {
			confirmations = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		} else {
			log.Warn(err)
		}
	}

	// Publish the message to the AMQP server
	err = channel.Publish(
		exchange,
		routingKey,
		mandatory,
		immediate,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     contentType,
			ContentEncoding: contentEncoding,
			Body:            body,
			DeliveryMode:    deliveryMode,
			Priority:        uint8(priority),
			Timestamp:       time.Now(),
			AppId:           apiKey,
		},
	)

	// Indicate to the caller than an error occurred on publish
	if err != nil {
		log.Error(err)
		w.WriteHeader(500)
		return
	}

	// Wait for confirmation that the message published successfully
	if confirm {
		log.Debugf("Waiting for publish confirmation")

		confirmed := <-confirmations
		if !confirmed.Ack {
			log.Error("Publish failed")
			w.WriteHeader(500)
			return
		}

		log.Debugf("Publish confirmed")
	}

	// If we make it here the message was successfully published
	w.WriteHeader(202)
}

func closeChannel(channel *amqp.Channel) {
	err := channel.Close()
	if err != nil {
		log.Warn(err)
	}
}
