package configs

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ReplayConfig struct {
	TopicParitionConfig       kafka.TopicPartition
	NumberOfMessageFromOffest int
	MsgReadTimeOut            int
}
