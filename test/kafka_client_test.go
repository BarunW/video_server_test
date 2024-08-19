package test

import (
	"fmt"
	"testing"
	kafkaclient "video_server/kafka_client"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestKafkaPublish(t *testing.T) {
	kfC, err := kafkaclient.NewKafkaClient(kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		t.Fail()
		return
	}

	fmt.Println("Testing Create Topic")
	topic := "TestTopic"
	if err := kfC.CreateTopic(topic); err != nil {
		t.Fail()
	}

	fmt.Println("Testing Publish")
	if err := kfC.Publish(topic, []byte("Hello World"), nil); err != nil {
		t.Fail()
	}

	fmt.Println("Testing Publish with serializeFunc")
	if err := kfC.Publish(topic, []byte("Hello World"), kafkaclient.SerializeToProtoBuf); err == nil {
		t.Fail()
	}

	t.Cleanup(func() {
		kfC.Close()
	})

}
