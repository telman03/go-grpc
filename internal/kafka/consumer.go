package kafka

import (
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	sessionTimeout = 7000
	pollTimeout    = 100 * time.Millisecond
)

type Handler interface {
	HandleMessage(message []byte, offset kafka.Offset) error
}

type Consumer struct {
	consumer *kafka.Consumer
	handler  Handler
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewConsumer(handler Handler, addresses []string, topic, consumerGroup string) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(addresses, ","),
		"group.id":                 consumerGroup,
		"session.timeout.ms":       sessionTimeout,
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		"auto.offset.reset":        "earliest",
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}

	if err := c.Subscribe(topic, nil); err != nil {
		return nil, err
	}
	return &Consumer{
		consumer: c,
		handler:  handler,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}, nil
}

func (c *Consumer) Start() {
	defer close(c.doneCh)
	for {
		select {
		case <-c.stopCh:
			logrus.Info("Stopping consumer...")
			return
		default:
		}

		kafkaMsg, err := c.consumer.ReadMessage(pollTimeout)
		if err != nil {
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
				continue
			}
			logrus.Error(err)
			continue
		}
		if kafkaMsg == nil {
			continue
		}
		logrus.Infof("Received message: %s", string(kafkaMsg.Value))
		if err := c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition.Offset); err != nil {
			logrus.Errorf("Failed to handle message: %v", err)
			continue
		}
		if _, err := c.consumer.StoreMessage(kafkaMsg); err != nil {
			logrus.Errorf("Failed to store message: %v", err)
			continue
		}
	}
}

func (c *Consumer) Stop() error {
	close(c.stopCh)
	<-c.doneCh
	if _, err := c.consumer.Commit(); err != nil {
		logrus.Errorf("Failed to commit offsets: %v", err)
	}
	return c.consumer.Close()
}
