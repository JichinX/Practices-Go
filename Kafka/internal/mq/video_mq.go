package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type VideoMQ struct {
	*MQ
}
type VideoMessage struct {
	Content string
}

func (v_mq *VideoMQ) Produce(message *VideoMessage) error {
	bytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	err = v_mq.Producer.WriteMessages(context.Background(), kafka.Message{Value: bytes})
	return err
}

func (v_mq *VideoMQ) Consume() {
	for {
		fmt.Println("----- consumer -----")
		m, err := v_mq.Consumer.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("[%s]获取消息失败：%s", v_mq.Name, err)
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}
