package main

import (
	"fmt"
	"log"
	"time"

	"practices.jichinx.github.io/kafka/internal/manage"
	"practices.jichinx.github.io/kafka/internal/mq"
)

func main() {
	brokers := []string{"localhost:9094"}
	km := NewKafkaManager(brokers)
	fmt.Println(km.Brokers)
	vm := InitVideoMessage(km)
	for {
		fmt.Println("+++++ produce +++++")
		err := vm.Produce(&mq.VideoMessage{Content: "This is message"})
		if err != nil {
			log.Fatal("vm.Produce: ", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func InitVideoMessage(km *manage.KafkaManager) mq.VideoMQ {
	videoMq := mq.VideoMQ{MQ: &mq.MQ{
		Topic: "topic_video", GroupId: "group_video",
	}}
	//在这初始化，减少 packages之间的引用
	videoMq.Consumer = km.NewConsumer(videoMq.Topic, videoMq.GroupId)
	videoMq.Producer = km.NewProducer(videoMq.Topic)
	fmt.Println(videoMq.Consumer, videoMq.Producer)
	go videoMq.Consume()
	return videoMq
}

func NewKafkaManager(brokers []string) *manage.KafkaManager {
	return &manage.KafkaManager{Brokers: brokers}
}
