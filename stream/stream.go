package stream

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"video_server/configs"
	"video_server/models"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	ffmpeg_go "github.com/u2takey/ffmpeg-go"
)

type chanWriter struct{
    frameChan chan []byte 
}


type stream struct{
    publisher publisher 
    serializeFunc func(i interface{}) ([]byte, error)
}

type publisher interface {
   Publish(topic string, data interface{}, pubFunc func(i interface{}) ([]byte, error))      
   CreateTopic(topic string, ts ...kafka.TopicSpecification) error
}

func NewStream(pub publisher, serializeFunc func(i interface{}) ([]byte, error)) *stream{
    return &stream{
        publisher: pub,
        serializeFunc: serializeFunc, 
    }
}

func(c *chanWriter) Write(p []byte) (int, error){
   c.frameChan <- p 
   // add meta data at byte level
   return len(p), nil 
}


func(s *stream) listenFrame(topic string, frameChan <- chan []byte ) {
    for{
        select{
        case imageData := <-frameChan:
            /* Testing */ 
            // go process(e)
            s.publisher.Publish(topic, imageData, nil)
        }
    }
}

/* 
    =========================
        Testing
    =========================
*/
var counter int = 0
func process(imgData []byte){
    f, err := os.Create("frame_"+strconv.Itoa(counter)) 
    if err != nil{
        fmt.Println("Failed to create the file", err.Error())
        return 
    }
    defer f.Close()

    n, err := f.Write(imgData)
    if err != nil{
        fmt.Println("Failed to write the file", err.Error())
        return
    }

    counter++
    fmt.Println("Bytes Written", n)

}
/* =========================== */ 

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

    go s.listenFrame(cam.Name, fChan)
	err := ffmpeg_go.Input(config.ConnURL).
        Output( 
            config.OutputFileName,
            config.OutputConfig,
        ).
		WithOutput(&cw).
		Run()
    if err != nil{
        slog.Error("Failed to spilt the frame", "Details", err.Error(), "URL", config.ConnURL)
        return
    }
    fmt.Println("----------------Done") 
    return
}


