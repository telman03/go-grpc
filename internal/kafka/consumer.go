package kafka

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	sessionTimeout = 7000
	noTimeout = -1
)

type Handler interface {
	HandleMessage(message []byte, offset kafka.Offset) error
}

type Consumer struct {
	consumer *kafka.Consumer
	handler Handler
	stop bool
}

func NewConsumer(handler Handler, addresses []string, topic, consumerGroup string) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(addresses, ","),
		"group.id":         consumerGroup,
		"session.timeout.ms": sessionTimeout,
		"enable.auto.offset.store": false,
		"enable.auto.commit": true,
		"auto.commit.interval.ms": 5000,
		"auto.offset.reset": "earliest",
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
		handler: handler,
	}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			logrus.Info("Stopping consumer...")
			break
		}
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			logrus.Error(err)
		}
		logrus.Infof("Received message: %s", string(kafkaMsg.Value))
		if kafkaMsg == nil {
			continue
		}
		if err := c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition.Offset); err != nil{
			logrus.Errorf("Failed to handle message: %v", err)
			continue
		}
		if _, err := c.consumer.StoreMessage(kafkaMsg); err != nil {
			logrus.Errorf("Failed to store message: %v", err)
			continue
		}
	}
}

func (c *Consumer) Stop() error{
	c.stop = true
	if _, err := c.consumer.Commit(); err != nil {
		logrus.Errorf("Failed to commit offsets: %v", err)
	}
	return c.consumer.Close()
}