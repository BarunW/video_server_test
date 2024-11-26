package test

import (
	"fmt"
	"net/http"
	"testing"
	"time"
	"video_server/misc"
)

/* 
=========================
Publishing the frame on each kafka topic for each rtsp url 
===========================
*/

func TestPublishOnTopic(t *testing.T) {
    
    topic := "Ftopic"
	rtspURLS, err := misc.GetRtspUrls()
	if err != nil {
		t.Fail()
		return
	}

	for i, url := range rtspURLS[:30]{
		httpUrl := fmt.Sprintf("http://localhost:8000/camera/register/%s%d?url=%s", topic, i, url)
		fmt.Println(httpUrl)
		resp, err := misc.MakeRequest(httpUrl)
		if err != nil || resp == nil {
			t.Fail()
		}
		if resp != nil {
			if resp.StatusCode != http.StatusOK {
				fmt.Println(resp.StatusCode)
				t.Fail()
			}
			resp.Body.Close()
		}
		<-time.After(1 * time.Second)
	}
}
