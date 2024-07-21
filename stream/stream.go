package stream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"video_server/configs"
	"video_server/models"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	ffmpeg_go "github.com/u2takey/ffmpeg-go"
)

type chanWriter struct{
    frameChan chan []byte 
}

type stream struct{
    wg *sync.WaitGroup
    publisher publisher 
    serializeFunc func(i interface{}) ([]byte, error)
}

type publisher interface {
   Publish(topic string, data interface{}, pubFunc func(i interface{}) ([]byte, error)) error
   CreateTopic(topic string, ts ...kafka.TopicSpecification) error
}

func NewStream(pub publisher, wg *sync.WaitGroup, serializeFunc func(i interface{}) ([]byte, error)) *stream{
    return &stream{
        wg: wg,
        publisher: pub,
        serializeFunc: serializeFunc, 
    }
}

func(c *chanWriter) Write(p []byte) (int, error){  
    if c.frameChan == nil{
        return 0, fmt.Errorf("Failed to Write to Nill Channel")
    }
    c.frameChan <- p 
    // add meta data at byte level
    return len(p), nil 
}


func(s *stream) listenFrame(ctx context.Context, topic string, frameChan <- chan []byte ) { 
    outer:
    for{
        select{
        case imageData := <-frameChan:
            /* Testing */ 
            // go process(e)
            s.publisher.Publish(topic, imageData, nil)
        case <-ctx.Done():
            break outer
        }
    }

    s.wg.Done()
    fmt.Println("Listen Frame Done")
    return 
}

func(s *stream) HandleRTSPStream(ctx context.Context, config configs.FFMPEG_RstpStreamConfig, cam models.Camera){ 
    fChan := make(chan []byte, 2)
    cw := chanWriter{
       frameChan: fChan, 
   }

    defer close(fChan)
    // get everything setup for the camera
    // creating a topic
    if err := s.publisher.CreateTopic(cam.Name); err != nil{
        slog.Error("Failed to create topic", "Details", err.Error())
        return
    }

    childContext, cancel := context.WithCancel(context.Background())
    
    s.wg.Add(1)
    go s.listenFrame(childContext, cam.Name, fChan)
    
	ffmpegStream := ffmpeg_go.Input(config.ConnURL).
        Output( 
            config.OutputFileName,
            config.OutputConfig,
        ).WithOutput(&cw)


        go func() {
            if err := ffmpegStream.Run(); err != nil{
                slog.Error("Failed to spilt the frame", "Details", err.Error(), "URL", config.ConnURL)
            }
            fmt.Println("FFMPEG STOP RUNNING")
        }()

    <-ctx.Done()
    ffmpegStream.Context.Done()  
    fmt.Println("Closing RTSP Stream connection")

    cancel()
    s.wg.Done()

    fmt.Println("----------------Done") 
    return
}


