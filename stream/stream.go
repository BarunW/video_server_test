package stream

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"video_server/configs"
	"video_server/misc"
	"video_server/models"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	ffmpeg_go "github.com/u2takey/ffmpeg-go"
)

type CustomWriter struct {
	buffer      []byte
	eoiChan     chan []byte
    //packetChan  chan []byte
    ctx         context.Context
    eoiIndex    int
    soiIndex    int 
}

type stream struct {
	publisher     publisher
	serializeFunc func(i interface{}) ([]byte, error)
}

type publisher interface {
    Publish(topic string, data interface{}, serializeFunc func(i interface{}) ([]byte, error)) error
    CreateTopic(topic string, ts ...kafka.TopicSpecification) error
    PublishOnParition(topic string, dataByt []byte, partition int32, camId int) error
    AssignTopicAndPartition(topic string) (int32, error)
    AssignTopicAndPartition2(topic string, noOfPartition int, noOfpubPerPart int) (int32, error)
}

func NewStream(pub publisher, serializeFunc func(i interface{}) ([]byte, error)) *stream {
	return &stream{
		publisher:     pub,
		serializeFunc: serializeFunc,
	}
}

const (
    soiMarker = "\xFF\xD8" // Start of Image
    eoiMarker = "\xFF\xD9" // End of Image
)


func (w *CustomWriter) Write(p []byte) (int, error) {
	w.buffer = append(w.buffer, p...)    
	// JPEG End of image marker (0xFFD9)
    outer:
	for {
        select{
        case <-w.ctx.Done():
            break outer
        default:
            w.eoiIndex = bytes.Index(w.buffer, []byte(eoiMarker)) 
            soiIndex   := bytes.Index(w.buffer, []byte(soiMarker))
            if w.eoiIndex == -1 {
                break outer
            }

            // soiIndex is where start of frame for jpeg/jpg 
            // here it filters the execess data e.g @@@ before 0XFFD8
            // from the reaming data of [w:eoiIndex+2]
            w.eoiChan <- w.buffer[soiIndex:w.eoiIndex+2]
            w.buffer = w.buffer[w.eoiIndex+2:]
        }
        
	} 
	return len(p), nil
}


func(cw *CustomWriter) listenToEoi(topic string, pub publisher, serializeFunc func(i interface{}) ([]byte, error)){
    for frame := range cw.eoiChan{
        err := pub.Publish(topic, frame, nil)
        if err != nil{
            slog.Error("Failed to published", "Details", err.Error())
            return
        }
    }
}

//var counter int = 0
func(cw *CustomWriter) listenToEoi2(topic string, pub publisher, partitionNo int32, camId int){
    for frame := range cw.eoiChan{
/* 
    // To check the frame data for completeness

	fName := fmt.Sprintf("packet%d", counter)	
   	f, err := os.Create(fName)  
	if err != nil{
		slog.Error("Failed to create File")
	}
	defer f.Close()
	if _, err := f.Write(frame); err != nil{
		slog.Error("Failed to write to the file", "Details", err.Error())
	}
	counter++ 
*/	
        err := pub.PublishOnParition(topic, frame, partitionNo, camId) 
        if err != nil{
            slog.Error("Failed to published", "Details", err.Error())
            return
        }
    }
}


func (s *stream) HandleRTSPStreamForPublishOnPartition(ctx context.Context, parentWg *sync.WaitGroup,
	config configs.FFMPEG_RstpStreamConfig, cam models.Camera) {
        var ( 
            wg sync.WaitGroup
            eoiChan = make(chan []byte, 64)	
        ) 

        writer := &CustomWriter{
            buffer:     make([]byte, 0),
            eoiChan: eoiChan,
            ctx: ctx,
        }

	defer close(eoiChan)

// - published to partition     
// - Assingning topic and partition 
    n, err := s.publisher.AssignTopicAndPartition(cam.Name)
    fmt.Println("CamerName: ", cam.Name)
     if err != nil{
        slog.Error("Failed to create topic", "Details", err.Error())
        return
    }

    go writer.listenToEoi2(cam.Name, s.publisher, n, misc.NewCamId()) 
    
	ffmpegStream := ffmpeg_go.Input(config.ConnURL, config.InputConfig).
		Output(
			config.OutputFileName,
			config.OutputConfig,
		).WithOutput(writer)

	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		if err := ffmpegStream.Run(); err != nil {
			slog.Error("Failed to spilt the frame", "Details", err.Error(), "URL", config.ConnURL)
		}
		fmt.Println("FFMPEG HAS STOP")
		wg.Done()
	}(&wg)

	<-ctx.Done()

	ffmpegStream.Context.Done()
	fmt.Println("Closing RTSP Stream connection")
	wg.Wait()

	parentWg.Done()
	fmt.Println("----------------Done")
	return
}

func (s *stream) HandleRTSPStreamForTopic(ctx context.Context, parentWg *sync.WaitGroup,
	config configs.FFMPEG_RstpStreamConfig, cam models.Camera) {
    var ( 
        wg sync.WaitGroup
	    eoiChan = make(chan []byte, 64)	
    ) 

	writer := &CustomWriter{
		buffer:     make([]byte, 0),
        eoiChan: eoiChan,
        //packetChan: packetChan,
        ctx: ctx,
	}

	defer close(eoiChan)
    	//defer close(packetChan)

// - pubished to topic 
    if err := s.publisher.CreateTopic(cam.Name); err != nil{
        slog.Error("Failed to create topic", "Details", err.Error())
        return
    }

    go writer.listenToEoi(cam.Name, s.publisher, s.serializeFunc) 

	ffmpegStream := ffmpeg_go.Input(config.ConnURL, config.InputConfig).
		Output(
			config.OutputFileName,
			config.OutputConfig,
		).WithOutput(writer)

	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		if err := ffmpegStream.Run(); err != nil {
			slog.Error("Failed to spilt the frame", "Details", err.Error(), "URL", config.ConnURL)
		}
		fmt.Println("FFMPEG HAS STOP")
		wg.Done()
	}(&wg)

	<-ctx.Done()

	ffmpegStream.Context.Done()
	fmt.Println("Closing RTSP Stream connection")
	wg.Wait()

	parentWg.Done()
	fmt.Println("----------------Done")
	return
}
