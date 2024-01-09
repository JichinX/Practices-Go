package mq

import (
	"github.com/segmentio/kafka-go"
)

type MQ struct {
	Name     string
	Topic    string
	GroupId  string
	Producer *kafka.Writer
	Consumer *kafka.Reader
}
