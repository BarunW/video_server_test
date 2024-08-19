package test

import (
	"fmt"
	"testing"
	"video_server/fbs"
	kafkaclient "video_server/kafka_client"
)

func TestFlatBuffers(t *testing.T) {
	byt, _ := kafkaclient.SerializeToFlatBuffers([]byte("hello world"))
	fd := fbs.GetRootAsImageFrame(byt, 0)
	if string(fd.DataBytes()) != "hello world" {
		fmt.Println(string(fd.DataBytes()))
		t.Fail()
	}

}
