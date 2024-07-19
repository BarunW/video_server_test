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
	"video_server/configs"
	kafkaclient "video_server/kafka_client"
	"video_server/models"
	"video_server/stream"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)


func main(){
 //"rtsp://localhost:8554/webcam"
    fmt.Println("Hello World")
    fmt.Println("CPU Number", runtime.NumCPU())
    runtime.GOMAXPROCS(runtime.NumCPU())

    kfC, err := kafkaclient.NewKafkaClient(kafka.ConfigMap{ "bootstrap.servers": "localhost:9092" } )    
    if err != nil{
        slog.Error("Failed to Set Up New Kafka Client", "Details", err.Error())
        return
    }  
    
    // streaming
    strm := stream.NewStream(kfC, kafkaclient.SerializeToFlatBuffers)

    serveMux := http.NewServeMux()

    server := http.Server{
        Addr: "localhost:8000", 
        Handler: serveMux, 
    }
    serveMux.HandleFunc("/debug/pprof/", http.HandlerFunc(pprof.Index))
    serveMux.HandleFunc("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
    serveMux.HandleFunc("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
    serveMux.HandleFunc("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
    serveMux.HandleFunc("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

    counter := 0

    parentContext, cancel := context.WithCancel(context.Background())
    camera := models.Camera{
       Name: "webcam", 
       ConnectionURL: "rtsp://localhost:8554/webcam", //"rtsp://192.168.43.135:8554/webcam",
    }
    
    serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Hello World"))
    })
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    basline := m.Alloc
    fmt.Printf("Baseline memory usage: %d bytes", basline)


    serveMux.HandleFunc("/camera/register/*", func(w http.ResponseWriter, r *http.Request) { 
        paths := strings.Split(r.URL.String(), "/")
        l := len(paths) - 1  
        fmt.Println("Cam Name",paths[l])
        if paths[l] == "" || paths[l] == " "{ 
            w.WriteHeader(http.StatusUnauthorized)
            return
        }

        bdy, err := io.ReadAll(r.Body)
        if err != nil{
           slog.Error("Failed to the read request body", "Details", err.Error()) 
           w.WriteHeader(http.StatusInternalServerError)
           return
        }
        defer r.Body.Close()
        fmt.Println("Req Body", string(bdy))

        // connect the rtsp stream a
        camera.Name = paths[l] 
        go strm.HandleRTSPStream(parentContext, 
                configs.NewFFMPEG_RTSPStreamConfig(camera.ConnectionURL), camera) 

        counter++

        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Hello, World!"))

        return
    })
    
    sigChan := make(chan os.Signal, 0)
    signal.Notify(sigChan, os.Interrupt)
    signal.Notify(sigChan, os.Kill)
    

    go func(){
        if err := server.ListenAndServe(); err != nil{
            slog.Error("Unable to listen and serve", "Details", err.Error())
            return 
        }
    }()

    <-sigChan
    runtime.ReadMemStats(&m)
    withGoroutine := m.Alloc
    fmt.Printf("Memory usage With  goRoutine: %d bytes", withGoroutine)

    cancel()
    kfC.Close()

    // Handle gracefull shutdown

}


