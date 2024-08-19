package test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"video_server/configs"
	kafkaclient "video_server/kafka_client"
	"video_server/models"
	"video_server/stream"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func BenchmarkRTSPConnection(b *testing.B) {
	var wg sync.WaitGroup

	kfC, err := kafkaclient.NewKafkaClient(kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		slog.Error("Failed to Set Up New Kafka Client", "Details", err.Error())
		return
	}

	parentContext, cancel := context.WithCancel(context.Background())

	// streaming
	strm := stream.NewStream(kfC, &wg, kafkaclient.SerializeToFlatBuffers)
	for i := 0; i < b.N; i++ {

		url := fmt.Sprintf("rtsp://localhost:8554/webcam")
		cameraName := fmt.Sprintf("camera_%d", i)
		camera := models.Camera{
			Name:          cameraName,
			ConnectionURL: url,
		}

		strm.HandleRTSPStream(parentContext, configs.NewFFMPEG_RTSPStreamConfig(url), camera)
	}

	b.Cleanup(func() {
		cancel()
		kfC.Close()
		wg.Wait()
	})
}
