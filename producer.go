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

func New(host string, port int) *Producer {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5

	brokerAddr := fmt.Sprintf("%s:%d", host, port)

	producer, err := sarama.NewSyncProducer([]string{brokerAddr}, cfg)
	if err != nil {
		log.Fatalf("Ошибка при создании продюсера: %v", err)
	}

	return &Producer{producer: producer}
}

func (p *Producer) SendAccountCreation(event AccountCreationEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("Ошибка сериализации AccountCreationEvent: %v", err)
	}
	p.sendToTopic(TopicAccountCreation, data)
}

func (p *Producer) SendAccountActivity(event AccountActivityEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("Ошибка сериализации AccountActivityEvent: %v", err)
	}
	p.sendToTopic(TopicAccountActivity, data)
}

func (p *Producer) sendToTopic(topic string, data []byte) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Ошибка при отправке сообщения: %v", err)
	}

	log.Printf("Сообщение отправлено в %s [partition=%d, offset=%d]", topic, partition, offset)
}

// Close закрывает соединение с брокером.
func (p *Producer) Close() {
	if err := p.producer.Close(); err != nil {
		log.Printf("Ошибка при закрытии продюсера: %v", err)
	}
}
