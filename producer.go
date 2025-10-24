package equi_genea_broker_producer

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
}

const (
	TopicAccountCreation = "account_creation_topic"
	TopicAccountActivity = "account_activity_topic"
)

func NewProducer(host, port string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5

	brokerAddr := fmt.Sprintf("%s:%d", host, port)

	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return &Producer{producer: producer}, nil
}

func (p *Producer) SendAccountCreation(event *AccountCreationEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal AccountCreationEvent: %w", err)
	}
	return p.sendToTopic(TopicAccountCreation, data)
}

func (p *Producer) SendAccountActivity(event *AccountActivityEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal AccountActivityEvent: %w", err)
	}
	return p.sendToTopic(TopicAccountActivity, data)
}

func (p *Producer) sendToTopic(topic string, data []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to topic %s: %w", topic, err)
	}

	log.Printf("Сообщение отправлено в %s [partition=%d, offset=%d]", topic, partition, offset)
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
