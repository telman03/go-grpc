package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/telman03/go-grpc/internal/handler"
	"github.com/telman03/go-grpc/internal/kafka"
)

const (
	topic = "my-topic"
	consumerGroup = "my-consumer-group"
)
var address = []string{"localhost:9092"}

func main() {
	h := handler.NewHandler()
	c, err := kafka.NewConsumer(h, address, topic, consumerGroup)
	if err != nil {
		logrus.Fatal(err)
	}
	
	go func() {
		c.Start()
	}()
	
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	
	<- signalChan
	logrus.Info("Received shutdown signal, stopping consumer...")
	logrus.Fatal(c.Stop())
}