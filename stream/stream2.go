package stream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"video_server/configs"
	"video_server/misc"
	"video_server/models"

	ffmpeg_go "github.com/u2takey/ffmpeg-go"
)


func(cw *CustomWriter) listenToEoi3(topic string, pub publisher, key int32 ){
    for frame := range cw.eoiChan{
        err := pub.PublishOnParition(topic, frame, key, misc.NewCamId())
        if err != nil{
            slog.Error("Failed to published", "Details", err.Error())
            return
        }
    }
}


func (s *stream) HandleRTSPStream2(ctx context.Context, parentWg *sync.WaitGroup,
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

// - published by grouping on partition or topic     
// - Assingning topic and partition 
    n, err := s.publisher.AssignTopicAndPartition2(cam.Name, 10, 3)
    fmt.Println("CamerName: ", cam.Name)
     if err != nil{
        slog.Error("Failed to create topic", "Details", err.Error())
        return
    }

    //go writer.assemblePacket()
    go writer.listenToEoi3(cam.Name, s.publisher, n ) 
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

