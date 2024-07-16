package kafkaclient

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
	"video_server/configs"
	"video_server/fbs"
	"video_server/protos"

	kf "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type kafkaClient struct {
	producer    *kf.Producer
	adminClient *kf.AdminClient
}

func SerializeToProtoBuf(i interface{} ) ([]byte, error){
    data, ok := i.(protoreflect.ProtoMessage)
    if !ok{ 
        return nil, fmt.Errorf("%s", "Invalid Type")
    }

    byte, err := proto.Marshal(data)
    if err != nil {
        slog.Error("Unable to serialize the proto message", "Details", err.Error())
        return nil, err
    }

    return byte, nil
}

var ids int32 = 0 

func SerializeToFlatBuffers(i interface{}) ([]byte, error){
    builder := flatbuffers.NewBuilder(1024)
    ids++ 
    data := builder.CreateByteVector(i.([]byte))
    timeStamp := builder.CreateString(time.Now().Format(time.RFC3339))
    cameraId := builder.CreateByteString([]byte("camera"))
    
    fbs.ImageFrameStart(builder)
    fbs.ImageFrameAddId(builder, ids)
    fbs.ImageFrameAddData(builder, data)
    fbs.ImageFrameAddCameraId(builder, cameraId)
    fbs.ImageFrameAddTimestamp(builder, timeStamp)

    fd := fbs.ImageFrameEnd(builder)
        
    builder.Finish(fd)
    return builder.FinishedBytes(), nil
    
}

func NewKafkaClient(configMap kf.ConfigMap) (*kafkaClient, error) {
	ac, err := kf.NewAdminClient(&configMap)
	if err != nil {
		slog.Error("Failed to create new adim client", "Details", err.Error())
	}

	now := time.Now()
	p, err := kf.NewProducer(&configMap)
	if err != nil {
		slog.Error("Failed to call producer api", "Details", err.Error())
		return nil, err
	}

	k := &kafkaClient{
		producer:    p,
		adminClient: ac,
	}
	k.msgReportHandler()
	fmt.Println("KAFKA Connection time", time.Since(now))
	return k, nil
}

func (ka *kafkaClient) msgReportHandler() {
	go func() {
		for e := range ka.producer.Events(){
			switch ev := e.(type) {
			case *kf.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
                    fmt.Println("--------------")
					fmt.Println("Message Delievered For Topic ", *ev.TopicPartition.Topic)
					fmt.Println("OFFSET", ev.TopicPartition.Offset, "PARTITION", ev.TopicPartition.Partition)
                    fmt.Println("---------------")
				}
			}
		}
	}()
}
func (ka *kafkaClient) CreateTopic(topic string, tS ...kf.TopicSpecification) error {
	if tS == nil {
		tS = []kf.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		}
	}

	ctx, _ := context.WithCancel(context.Background())
	result, err := ka.adminClient.CreateTopics(ctx, tS)
	if err != nil {
		if err.(kf.Error).Code() == kf.ErrTopicAlreadyExists {
			fmt.Println("Not a problem", err)
			return nil
		}
		slog.Error("Failed to create Topic", "Details", err.Error())
	}

	for _, r := range result {
		fmt.Println(r)
	}
	return nil
}

func(ka *kafkaClient) Close(){
    ka.producer.Close()
}

func (ka *kafkaClient) Publish(topic string, data interface{}, serializeFunc func(i interface{}) ([]byte, error)) {
    var dataByt []byte
    if serializeFunc != nil{ dataByt, _ = serializeFunc(data) }

    dataByt = data.([]byte)

	now := time.Now()
    msg := kf.Message{
			TopicPartition: kf.TopicPartition{Topic: &topic, Partition: kf.PartitionAny},
			Value:          dataByt,
            Timestamp: time.Now(),
            TimestampType: kf.TimestampCreateTime,
            Headers: []kf.Header{
                {
                    Key: "timeStamp",
                    Value: []byte(time.Now().Format(time.RFC3339)),
                },
                {
                    Key: "cameraName",
                    Value: []byte(topic),
                },
            },
    }
    msg.Timestamp = time.Now().In(time.Local)
	err := ka.producer.Produce(&msg, nil)
	if err != nil {
		slog.Error("KAFKA Failed to publish message", "Details", err.Error())
	}
	fmt.Println("KAFKA", topic, time.Since(now))
    ka.producer.Flush(1000)
}

func deserializeProtoBuf(byt []byte) (*protos.FrameData, error) {
	fd := protos.FrameData{}
	if err := proto.Unmarshal(byt, &fd); err != nil {
		return nil, err
	}
	return &fd, nil
}

func (ka *kafkaClient) Subscribe(topic string) {
	c, err := kf.NewConsumer(&kf.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":            topic + "Group2",
		"enable.auto.commit": false,
		"auto.offset.reset":    "earliest",
	})

	if err != nil {
		panic(err)
	}

	if err := c.Subscribe(topic, nil); err != nil {
		slog.Error("Failed to subscribe to this topic", "Details", err.Error())
		return
	}
	run := true
	for run {
		msg, err := c.ReadMessage(250 * time.Millisecond)
		if err == nil {
			fmt.Println(topic)
			now := time.Now()
			fd, err := deserializeProtoBuf(msg.Value)
			if err != nil {
				slog.Error("Failed to deserialze the msg value", "Details", err.Error())
				return
			}
            fmt.Printf("MSG TS: %s FRAME META DATA %+v\n", msg.Timestamp.Format("2006-01-02T15:04:05.000Z07:00"), fd.MetaData)
			fmt.Println("---------------------------------", time.Since(now))
			ka.ack(c)
		} else if !err.(kf.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	c.Close()
}

func (ka *kafkaClient) ack(cons *kf.Consumer) {
	partitions, err := cons.Commit()
	if err != nil {
		slog.Error("Failed to ack", "Details", err.Error())
	}
	fmt.Printf("%+v", partitions)
}
func handleWriter(fd *protos.FrameData, counter int) {
	fOpenTime := time.Now()
	f, err := os.OpenFile("theoutput.png"+strconv.Itoa(counter), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		slog.Error("Unable to open file output.png", "Details", err.Error())
		return
	}
	defer f.Close()
	fmt.Println("File Open Time", time.Since(fOpenTime))

	n, err := f.Write(fd.Data)
	if err != nil {
		slog.Error("Failed to write the data to output.png", "Details", err.Error())
		return
	}
	fmt.Printf("%d Byte return", n)
}

// Replay with time stamp
func KafkaReplayFromTimeStamp(topic *string, totalMsg uint16) error {
	config := &kf.ConfigMap{
		"metadata.broker.list": "localhost:9092",
		"auto.offset.reset":    "earliest",
		"group.id":             *topic + "Group",
	}
	c, err := kf.NewConsumer(config)
	if err != nil {
		slog.Error("Failed to create new consumer", "Details", err.Error())
	}

	t, err := time.Parse("2006-01-02T15:04:05.000Z07:00", "2024-07-09T10:15:01.485-04:00")
	if err != nil {
		slog.Error("Failed to parse the time", "Details", err.Error())
        return err
	}

	fmt.Println(t)

	partitions := []kf.TopicPartition{
		{
			Topic:     topic,
			Partition: 0,
			Offset:    kf.Offset(30),
		},
	}
	offsets, err := c.OffsetsForTimes(partitions, 10000)
	if err != nil {
		slog.Error("Unable to set offsets for times", "Details", err.Error())
        fmt.Println(err)
		return err
	}

	for _, off := range offsets {
		fmt.Println("OFFSET", kf.Offset(30).String(), off)
	}
	if err := c.Assign(offsets); err != nil {
		slog.Error("Failed to assigned the offsets", "Details", err.Error())
	}

	ps, err := c.SeekPartitions(offsets)
	if err != nil {
		slog.Error("Failed to seek paritions")
		return err
	}
	for _, of := range ps {
		fmt.Println("-------- : < ) -----------", of.Topic, of.Partition)
	}

	counter := 0
    for i := 0; i < int(totalMsg); {
		ev := c.Poll(100)
		switch e := ev.(type) {
		case *kf.Message:
			fd, err := deserializeProtoBuf(e.Value)
			if err != nil {
				fmt.Println("Failed to deserialize protobuf", err.Error())
				return err
			}
			handleWriter(fd, counter)
            i++
			counter++
			fmt.Println(fd.MetaData.Timestamp.AsTime().Format(time.RFC3339))

		case kf.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
            return err
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	return nil
}

func ReplayWithOffest(replayConfig configs.ReplayConfig){
     c, err := kf.NewConsumer(&kf.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "group.id":          "myGroup",
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        panic(err)
    }

    defer c.Close()

    // Assign the consumer to the specific partition and offset
    err = c.Assign([]kf.TopicPartition{replayConfig.TopicParitionConfig})
    if err != nil {
        panic(err)
    }

//     Read the message at the specified offset
 //    msg, err := c.ReadMessage(5  * time.Second)
 //    if err != nil {
 //        fmt.Printf("Failed to read message: %v\n", err)
 //    } else {
 //        fd, _ := deserializeProtoBuf(msg.Value)
 //        handleWriter(fd, 96)
 //        fmt.Printf("Timestamp: %s\n", msg.Timestamp.Format("2006-01-02T15:04:05.000Z07:00"))
 //    }
 	counter := 0
    for counter < replayConfig.NumberOfMessageFromOffest{
		ev := c.Poll(replayConfig.MsgReadTimeOut)
		switch e := ev.(type) {
		case *kf.Message:
			fd, err := deserializeProtoBuf(e.Value)
			if err != nil {
				fmt.Println("Failed to deserialize protobuf", err.Error())
			}
			handleWriter(fd, counter)
			counter++
			fmt.Println(fd.MetaData.Timestamp.AsTime().Format(time.RFC3339))
            fmt.Println(e.TopicPartition.Offset)

		case kf.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

}

