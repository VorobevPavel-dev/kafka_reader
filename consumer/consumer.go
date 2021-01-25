package consumer

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/Shopify/sarama"
)

type GroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()
}

type Group struct {
	cg sarama.ConsumerGroup
}

func NewConsumerGroup(brokers []string, topics []string, groupID string, version sarama.KafkaVersion, handler GroupHandler) (*Group, error) {
	ctx := context.Background()
	cfg := sarama.NewConfig()
	cfg.Version = version
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	//Here we can do it cluster-wide
	client, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Panicf("[consumer.go][NewConsumerGroup] Cannot create new consumer group %v", err)
	}
	go func() {
		for {
			err := client.Consume(ctx, topics, handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					log.Panicf("[consumer.go][NewConsumerGroup] Fatal error occured: %v", err)
				}
			}
			if ctx.Err() != nil {
				return
			}
			handler.Reset()
		}
	}()
	handler.WaitReady()
	return &Group{
		cg: client,
	}, nil
}

func (c *Group) Close() error {
	return c.cg.Close()
}

type SessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

func StartBatchConsumer(brokers []string, topic string,
	maxBufferSize int, kafkaVersion string, groupID string,
	bufferCapacity int, tickerIntervalSeconds int,
	outputTarget string, outputAddress string) (*Group, error) {
	var count int64
	var start = time.Now()
	handler := NewBatchGroupHandler(
		&BatchConfig{
			MaxBufferSize: maxBufferSize,
			Callback: func(messages []*SessionMessage) error {
				for i := range messages {
					messages[i].Session.MarkMessage(messages[i].Message, "")
				}
				count += int64(len(messages))
				if count%5000 == 0 {
					log.Tracef("Batch consumer consumed %d messages at speed %.2f/s\n", count, float64(count)/time.Since(start).Seconds())
				}
				return nil
			},
			Output: OutputSettings{
				Target:        outputTarget,
				TargetAddress: outputAddress,
			},
		},
		bufferCapacity,
		tickerIntervalSeconds,
	)
	//Trying to parse kafka version
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		log.Errorf("Cannot parse kafka version:%v", err)
	}
	consumer, err := NewConsumerGroup(brokers, []string{topic}, groupID, version, handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
