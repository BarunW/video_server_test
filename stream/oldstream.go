package stream

//import (
//	"bytes"
//	"context"
//	"fmt"
//	"log/slog"
//	"sync"
//	"video_server/configs"
//	"video_server/models"
//
//	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
//	ffmpeg_go "github.com/u2takey/ffmpeg-go"
//)
//
//type CustomWriter struct {
//	buffer     []byte
//	dataBuffer *bytes.Buffer
//	eoiChan    chan struct{}
//	ctx        context.Context
//}
//
//type stream struct {
//	publisher     publisher
//	serializeFunc func(i interface{}) ([]byte, error)
//}
//
//type publisher interface {
//	Publish(topic string, data interface{}, serializeFunc func(i interface{}) ([]byte, error)) error
//	CreateTopic(topic string, ts ...kafka.TopicSpecification) error
//    	PublishOnParition(topic string, dataByt []byte, partition int32) error
//    	AssignTopicAndPartition() (int32, error)
//
//}
//
//func NewStream(pub publisher, serializeFunc func(i interface{}) ([]byte, error)) *stream {
//	return &stream{
//		publisher:     pub,
//		serializeFunc: serializeFunc,
//	}
//}
//
//var endOfImage = []byte{0xFF, 0xD9}
//
//func (w *CustomWriter) Write(p []byte) (int, error) {
//	w.buffer = append(w.buffer, p...)
//	// JPEG End of image marker (0xFFD9)
//outer:
//	for {
//		select {
//		case <-w.ctx.Done():
//			break outer
//		default:
//			eoiIndex := bytes.Index(w.buffer, endOfImage)
//			if eoiIndex == -1 {
//				break outer
//			}
//
//			frame := w.buffer[:eoiIndex+2]
//
//			w.dataBuffer.Write(frame)
//
//			w.eoiChan <- struct{}{}
//
//			w.buffer = w.buffer[eoiIndex+2:]
//		}
//
//	}
//
//	return len(p), nil
//}
//
//func (cw *CustomWriter) listenToEoi(topic string, pub publisher, serializeFunc func(i interface{}) ([]byte, error)) {
//	for range cw.eoiChan {
//		err := pub.Publish(topic, cw.dataBuffer.Bytes(), nil)
//		if err != nil {
//			slog.Error("Failed to published", "Details", err.Error())
//			return
//		}
//		cw.dataBuffer.Reset()
//	}
//}
//
//func(cw *CustomWriter) listenToEoi2(topic string, pub publisher, partitionNo int32){
//    for range cw.eoiChan{
//        err := pub.PublishOnParition(topic, cw.dataBuffer.Bytes(), partitionNo) 
//        if err != nil{
//            slog.Error("Failed to published", "Details", err.Error())
//            return
//        }
//        cw.dataBuffer.Reset()
//    }
//}
//
//
//func (s *stream) HandleRTSPStream(ctx context.Context, parentWg *sync.WaitGroup,
//	config configs.FFMPEG_RstpStreamConfig, cam models.Camera) {
//	var wg sync.WaitGroup
//
//	eoiChan := make(chan struct{}, 2)
//	dataBuffer := make([]byte, 10240)
//
//	writer := &CustomWriter{
//		buffer:     make([]byte, 10240),
//		dataBuffer: bytes.NewBuffer(dataBuffer),
//		eoiChan:    eoiChan,
//		ctx:        ctx,
//	}
//
//	defer close(eoiChan)
//// get everything setup for the camera
//// creating a topic for each camera
////    if err := s.publisher.CreateTopic(cam.Name); err != nil{
////        slog.Error("Failed to create topic", "Details", err.Error())
////        return
////    }
//
//// Assingning topic and partition 
//    n, err := s.publisher.AssignTopicAndPartition()
//     if err != nil{
//        slog.Error("Failed to create topic", "Details", err.Error())
//        return
//    }
//// - pubished to topic 
////    go writer.listenToEoi(cam.Name, s.publisher, s.serializeFunc)
//
//// - published to partition     
//     go writer.listenToEoi2(cam.Name, s.publisher, n)
//
//	ffmpegStream := ffmpeg_go.Input(config.ConnURL, config.InputConfig).
//		Output(
//			config.OutputFileName,
//			config.OutputConfig,
//		).WithOutput(writer)
//
//	wg.Add(1)
//	go func(wg *sync.WaitGroup) {
//		if err := ffmpegStream.Run(); err != nil {
//			slog.Error("Failed to spilt the frame", "Details", err.Error(), "URL", config.ConnURL)
//		}
//		fmt.Println("FFMPEG HAS STOP")
//		wg.Done()
//	}(&wg)
//
//	<-ctx.Done()
//
//	ffmpegStream.Context.Done()
//	fmt.Println("Closing RTSP Stream connection")
//	wg.Wait()
//
//	parentWg.Done()
//	fmt.Println("----------------Done")
//	return
//}
