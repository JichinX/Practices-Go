package manage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaManager struct {
	Brokers []string
}

func (manager *KafkaManager) NewProducer(topic string) *kafka.Writer {

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      manager.Brokers,
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		WriteTimeout: 1 * time.Second,
	})
	w.AllowAutoTopicCreation = true
	return w
}

func (manager *KafkaManager) NewConsumer(topic string, groupId string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     manager.Brokers,
		GroupID:     groupId,
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
	})
}
func ProduceMessage(producer *kafka.Writer, message interface{}) error {
	bytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return producer.WriteMessages(context.Background(), kafka.Message{Value: bytes})
}
