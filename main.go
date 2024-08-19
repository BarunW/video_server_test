package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"video_server/configs"
	kafkaclient "video_server/kafka_client"
	"video_server/models"
	"video_server/stream"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	//"rtsp://localhost:8554/webcam"
	fmt.Println("Hello World")
	fmt.Println("CPU Number", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	//ballast
	//    _= make([]byte, 10<<30)

	var wg sync.WaitGroup

	kfC, err := kafkaclient.NewKafkaClient(kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		slog.Error("Failed to Set Up New Kafka Client", "Details", err.Error())
		return
	}
    
	// streaming
	strm := stream.NewStream(kfC, kafkaclient.SerializeToFlatBuffers)

	serveMux := http.NewServeMux()

	server := http.Server{
		Addr:    "localhost:8000",
		Handler: serveMux,
	}

	serveMux.HandleFunc("/debug/pprof/", http.HandlerFunc(pprof.Index))
	serveMux.HandleFunc("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	serveMux.HandleFunc("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	serveMux.HandleFunc("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	serveMux.HandleFunc("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	parentContext, cancel := context.WithCancel(context.Background())

	serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello World"))
	})

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	basline := m.Alloc
	fmt.Printf("Baseline memory usage: %d bytes\n", basline)

	serveMux.HandleFunc("/camera/register/", HandleCameraReg(
        parentContext, &wg, strm.HandleRTSPStreamForTopic, 
    ))

    // For frame pushing on paritition
	serveMux.HandleFunc("/camera/register-partition/", HandleCameraReg(
        parentContext, &wg, strm.HandleRTSPStreamForPublishOnPartition, 
    ))

	serveMux.HandleFunc("/camera/register-gp/", HandleCameraReg(
        parentContext, &wg, strm.HandleRTSPStream2, 
    ))

	sigChan := make(chan os.Signal, 0)
	signal.Notify(sigChan, os.Interrupt)
	signal.Notify(sigChan, os.Kill)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			slog.Error("Unable to listen and serve", "Details", err.Error())
			return
		}
	}()

	<-sigChan
	runtime.ReadMemStats(&m)
	withGoroutine := m.Alloc
	fmt.Printf("Memory usage With  goRoutine: %d bytes", withGoroutine)

	// Handle gracefull shutdown
	cancel()
	kfC.Close()
	wg.Wait()
	if err := kafkaclient.WriteToCsv(); err != nil {
		slog.Error("Failed to write the data to csv")
	}
}

type handleRTSPfunc func(parentContext context.Context, wg *sync.WaitGroup, 
                                    config configs.FFMPEG_RstpStreamConfig, cam models.Camera) 

func HandleCameraReg(parentContext context.Context, wg *sync.WaitGroup, fn handleRTSPfunc ) func(w http.ResponseWriter, r *http.Request){
        return func(w http.ResponseWriter, r *http.Request) {
		paths := strings.SplitN(r.URL.String(), "/", 4)
		l := len(paths) - 1

		cameraDetails := strings.Split(paths[l], "?")

		url := strings.SplitN(cameraDetails[len(cameraDetails)-1], "=", 2)

		if paths[l] == "" || paths[l] == " " {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		bdy, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("Failed to the read request body", "Details", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		fmt.Println("Req Body", string(bdy))

		// connect the rtsp stream a
		camera := models.Camera{
			Name:          cameraDetails[0],
			ConnectionURL: url[len(url)-1],
		}

		if len(os.Args) >= 2 {
			fmt.Println(os.Args[1])
			if os.Args[1] == "same_topic" {
				camera.Name = os.Args[1]
			}
		}

		wg.Add(1)
		go fn(parentContext, wg,
			configs.NewFFMPEG_RTSPStreamConfig(camera.ConnectionURL), camera)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Request Successfully"))

		return
	}
}

