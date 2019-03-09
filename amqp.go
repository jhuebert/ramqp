package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

var (
	amqpConnection *amqp.Connection
	amqpCloseError chan *amqp.Error
)

func connect(uri *string) {
	// create the amqp error channel
	amqpCloseError = make(chan *amqp.Error)

	// run the callback in a separate thread
	go amqpConnector(*uri)

	// establish the amqp connection by sending
	// an error and thus calling the error callback
	amqpCloseError <- amqp.ErrClosed
}

func establishConnection(uri string) *amqp.Connection {
	for {
		connection, err := amqp.Dial(uri)
		if err == nil {
			return connection
		}

		log.Error(err)
		log.Warnf("Trying to reconnect")
		time.Sleep(500 * time.Millisecond)
	}
}

func amqpConnector(uri string) {
	for {
		amqpError := <-amqpCloseError
		if amqpError != nil {
			log.Infof("Connecting to %v", uri)
			amqpConnection = establishConnection(uri)
			amqpCloseError = make(chan *amqp.Error)
			amqpConnection.NotifyClose(amqpCloseError)
		}
	}
}
