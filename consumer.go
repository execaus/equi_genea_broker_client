package equi_genea_broker_client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

type Consumer struct {
	group                sarama.ConsumerGroup
	brokers              []string
	groupID              string
	accountCreationTopic string
	accountActivityTopic string
	closeOnce            sync.Once
}

func NewConsumer(host, port string, groupID string, config *sarama.Config) (*Consumer, error) {
	group, err := sarama.NewConsumerGroup([]string{fmt.Sprintf("%s:%s", host, port)}, groupID, config)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		group:                group,
		brokers:              []string{fmt.Sprintf("%s:%s", host, port)},
		groupID:              groupID,
		accountCreationTopic: TopicAccountCreation,
		accountActivityTopic: TopicAccountActivity,
	}, nil
}

func (c *Consumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.group.Close()
	})
	return err
}

func (c *Consumer) ConsumeAccountCreation(handler func(*AccountCreationEvent) error) error {
	return c.consumeTopic(c.accountCreationTopic, func(msg *sarama.ConsumerMessage) error {
		var event AccountCreationEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return err
		}
		return handler(&event)
	})
}

func (c *Consumer) ConsumeAccountActivity(handler func(*AccountActivityEvent) error) error {
	return c.consumeTopic(c.accountActivityTopic, func(msg *sarama.ConsumerMessage) error {
		var event AccountActivityEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return err
		}
		return handler(&event)
	})
}

func (c *Consumer) consumeTopic(topic string, handleMessage func(*sarama.ConsumerMessage) error) error {
	ctx := context.Background()
	handler := &consumerGroupHandler{handleMessage: handleMessage}
	for {
		err := c.group.Consume(ctx, []string{topic}, handler)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

type consumerGroupHandler struct {
	handleMessage func(*sarama.ConsumerMessage) error
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.handleMessage(msg); err != nil {
			return err
		}
		session.MarkMessage(msg, "")
	}
	return nil
}
