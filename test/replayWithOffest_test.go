package test

import (
	"fmt"
	"testing"
	"video_server/configs"
	kafkaclient "video_server/kafka_client"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

 //func TestReplayWithOffest(t *testing.T) {
 //	topic := "topic_1"
 //	replayConfig := configs.ReplayConfig{
 //		TopicParitionConfig: kafka.TopicPartition{
 //			Topic:     &topic,
 //			Partition: int32(0),
 //			Offset:    13440,
 //		},
 //		NumberOfMessageFromOffest: 100,
 //		MsgReadTimeOut:            100,
 //	}
 //	err := kafkaclient.ReplayWithOffest(replayConfig, 0, "topic_1_P")
 //	if err != nil {
 //		fmt.Println(err)
 //		t.Fail()
 //	}
 //}
 //

func TestReplayWithOffest(t *testing.T) {
	topic := "chunk"
	paritionToOffset := map[int32]kafka.Offset{
        0 : 0,
	}

	for key, val := range paritionToOffset{
		replayConfig := configs.ReplayConfig{
			TopicParitionConfig: kafka.TopicPartition{
				Topic:     &topic,
				Partition: key,
				Offset:    val,
			},
			NumberOfMessageFromOffest: 10,
			MsgReadTimeOut:            100,
		}
		err := kafkaclient.ReplayWithOffest(replayConfig, int(key), topic+"_P")
		if err != nil {
			fmt.Println(err)
			t.Fail()
		}
	}
}

