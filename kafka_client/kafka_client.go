package kafkaclient

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
	"video_server/configs"
	"video_server/fbs"
	"video_server/models"
	"video_server/protos"

	kf "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type kafkaClient struct {
	producer    *kf.Producer
	adminClient *kf.AdminClient
	logChan  chan []string
	counter int
	partitionNo int32
}

/*
var ProducerRecords = [][]string{
	{"partition", "offsets", "sent_at", "size"},
}*/


func SerializeToProtoBuf(i interface{}) ([]byte, error) {
	data, ok := i.(protoreflect.ProtoMessage)
	if !ok {
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

func SerializeToFlatBuffers(i interface{}) ([]byte, error) {
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
		return nil, err
	}

	now := time.Now()
	p, err := kf.NewProducer(&configMap)
	if err != nil {
		slog.Error("Failed to call producer api", "Details", err.Error())
		return nil, err
	}

	logChan := make(chan []string, 8)

	k := &kafkaClient{
		producer:    p,
		adminClient: ac,
		logChan : logChan,
		partitionNo : 0,
	}

	k.msgReportHandler()
	go k.logger()

	fmt.Println("KAFKA Connection time", time.Since(now))
	return k, nil
}


var ProducerRecords = [][]string{
	{"topic", "offsets", "sent_at", "size"},
}
func WriteToCsv() error {
	f, err := os.Create("producerRecord_onTopic.csv")
	if err != nil {
		slog.Error("Failed to create File producer", "Details", err.Error())
		return err
	}

	defer f.Close()

	csvWriter := csv.NewWriter(f)
	if err := csvWriter.WriteAll(ProducerRecords); err != nil {
		slog.Error("Failed to write the data to csv", "Details", err.Error())
		return err
	}

	return nil
}

func(ka *kafkaClient) logger(){
	f, err := os.Create("producer_on_record.csv")
	if err != nil{
		panic(err)	
	}
	w := csv.NewWriter(f)
	defer func(){
		w.Flush()
		f.Close() 	
	}()
	
	if ka.logChan == nil{
		panic(fmt.Errorf("Log Chan is Nill"))
	}

	for log := range ka.logChan{
		if err := w.Write(log); err != nil{
			slog.Error("Failed to write the log", "Details", err.Error())
		}
	}
}

func (ka *kafkaClient) msgReportHandler() {
    var (
            record []string
            prevTime int64  
            lag int
            now int64
        )
	go func() {
		for e := range ka.producer.Events() {
			switch ev := e.(type) { 
			case *kf.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					now = time.Now().UnixNano()             
                    lag = int(now - prevTime)
                    prevTime = now 
    
                    record = []string{
                       *ev.TopicPartition.Topic, 
                       ev.TopicPartition.String(),
                       ev.TopicPartition.Offset.String(), 
                       strconv.Itoa(int(now)),
                       strconv.Itoa(lag), 
                    }
		    ka.logChan <- record 
		    fmt.Println("--------------")
		    fmt.Println("Message Delievered For Topic ", *ev.TopicPartition.Topic)
		    fmt.Println("OFFSET", ev.TopicPartition.Offset, "PARTITION", ev.TopicPartition.Partition)
				}
			}
		}
		fmt.Println("Message Handler Done [[[[]]]]]")
		return
	}()

}


func (ka *kafkaClient) PublishOnParition(topic string, dataByt []byte, partition int32, camId int) error {
    
	fmt.Println("Message Received")
	now := time.Now()
	ts := strconv.Itoa(int(now.UnixNano()))
    
    	id  := "CAM-ID "+ strconv.Itoa(camId)
	msg := kf.Message{
		TopicPartition: kf.TopicPartition{Topic: &topic, Partition: partition},
		Value:          dataByt,
		TimestampType:  kf.TimestampType(int(now.UnixNano())),
		Headers: []kf.Header{
			{
				Key: "timeStamp",
				// let the consumer parse the time
				Value: []byte(ts),
			},
			{
				Key:  "camId",
				Value: []byte(id),
			},
            {
                Key: "type",
                Value: []byte{models.PUB_ON_PARTITION},
            },
		},
	}

	err := ka.producer.Produce(&msg, nil)
	if err != nil {
		slog.Error("KAFKA Failed to publish message", "Details", err.Error())
		return err
	}
	// this flush is for the message doesn't buffer in the kafka queue
	// making sure the message is sent
	ka.producer.Flush(1000)

	return nil
}

func (ka *kafkaClient) AssignTopicAndPartition(topic string) (int32, error) {
    tS := []kf.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     60,
				ReplicationFactor: 1,
			},
		}	
    

	ctx, _ := context.WithCancel(context.Background())
	result, err := ka.adminClient.CreateTopics(ctx, tS)
	if err != nil {
		if err.(kf.Error).Code() == kf.ErrTopicAlreadyExists {
			fmt.Println("Not a problem", err)
			return ka.partitionNo, nil
		}
		slog.Error("Failed to create Topic", "Details", err.Error())
        return -1, err
	}

    	ka.partitionNo += 1

	for _, r := range result {
		fmt.Println(r)
	}
	return ka.partitionNo, nil 
}

func (ka *kafkaClient) AssignTopicAndPartition2(topic string, noOfPartition int, noOfpubPerPart int) (int32 , error) {
    tS := []kf.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     noOfPartition,
				ReplicationFactor: 1,
			},
		}
        
    ka.counter += 1  
    if ka.counter > noOfpubPerPart{
        ka.partitionNo += 1
        ka.counter = 0
    }
    
    fmt.Println("COUNTER AND PARTITION NO", ka.counter, ka.partitionNo)
    
	ctx, _ := context.WithCancel(context.Background())
	result, err := ka.adminClient.CreateTopics(ctx, tS)
	if err != nil {
		if err.(kf.Error).Code() == kf.ErrTopicAlreadyExists {
			fmt.Println("Not a problem", err)
			return ka.partitionNo, nil
		}
		slog.Error("Failed to create Topic", "Details", err.Error())
        return -1, err
	}

	for _, r := range result {
		fmt.Println(r)
	}
	return ka.partitionNo, nil 
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

func (ka *kafkaClient) Close() {
	ka.adminClient.Close()
	ka.producer.Close()
	close(ka.logChan)
}

func (ka *kafkaClient) Publish(topic string, data interface{},
	serializeFunc func(i interface{}) ([]byte, error)) error {
	var dataByt []byte
	if serializeFunc != nil {
		var err error
		dataByt, err = serializeFunc(data)
		if err != nil {
			slog.Error("Failed to serializeFunc and data is not matching", "Details", err.Error())
			return err
		}
	} else {
		var ok bool
		dataByt, ok = data.([]byte)
		if !ok {
			slog.Error("The data should be type []byte")
			return fmt.Errorf("The data should be type []byte")
		}
	}
	now := time.Now()
	ts := strconv.Itoa(int(now.UnixNano()))
	msg := kf.Message{
		TopicPartition: kf.TopicPartition{Topic: &topic, Partition: kf.PartitionAny},
		Value:          dataByt,
		TimestampType:  kf.TimestampType(int(now.UnixNano())),
		Headers: []kf.Header{
			{
				Key: "timeStamp",
				// let the consumer parse the time
				Value: []byte(ts),
			},
			{
				Key:   "camId",
				Value: []byte(topic),
			},
            {
                Key: "type",
                Value: []byte{models.PUB_ON_TOPIC},
            },
		},
	}

	err := ka.producer.Produce(&msg, nil)
	if err != nil {
		slog.Error("KAFKA Failed to publish message", "Details", err.Error())
		return err
	}
	fmt.Println("KAFKA", topic, time.Since(now))

	// this flush is for the message doesn't buffer in the kafka queue
	// making sure the message is sent
	ka.producer.Flush(1000)
	return nil
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
		"group.id":           topic + "Group2",
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
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
		msg, err := c.ReadMessage(350 * time.Millisecond)
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

/*
=========================
	Testing
=========================
*/
var counter int = 0

func OutputToFile(imgData []byte, offset int, cameraNo int, name string) (int, error) {
	fileName := fmt.Sprintf("%s%dframe_offset_%d", name, cameraNo, offset+counter)
	f, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Failed to create the file", err.Error())
		return -1, err
	}

	go f.Close()

	n, err := f.Write(imgData)
	if err != nil {
		fmt.Println("Failed to write the file", err.Error())
		return -1, err
	}

	counter++
	return n, nil
}

/* =========================== */

func ReplayWithOffest(replayConfig configs.ReplayConfig, cameraNo int, name string) error {
	c, err := kf.NewConsumer(&kf.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup"+name,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		slog.Error("Failed to create New Consumer", "Details", err.Error())
		return err
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
	for counter := 0; counter < replayConfig.NumberOfMessageFromOffest; counter++ {
		ev := c.Poll(replayConfig.MsgReadTimeOut)
		switch e := ev.(type) {
		case *kf.Message:
			//			fd, err := deserializeProtoBuf(e.Value)
			//			if err != nil {
			//				fmt.Println("Failed to deserialize protobuf", err.Error())
			//			}
			//			handleWriter(fd, counter)
			//			counter++
			//			fmt.Println(fd.MetaData.Timestamp.AsTime().Format(time.RFC3339))
			fmt.Println(e.Value)
			OutputToFile(e.Value, int(replayConfig.TopicParitionConfig.Offset), cameraNo, name)

		case kf.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
	return nil

}
