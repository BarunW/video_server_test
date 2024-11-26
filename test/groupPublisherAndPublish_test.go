package test

import (
	"fmt"
	"net/http"
	"testing"
	"time"
	"video_server/misc"
)

func TestGroupPublishAndPubOnPartition(t *testing.T) {
    rtspUrls, err := misc.GetRtspUrls()
    if err != nil{
        t.Fail()
    }
	
    topic := "chunk2" 
    
    for _, url := range rtspUrls[:3]{
		httpUrl := fmt.Sprintf("http://localhost:8000/camera/register-gp/%s?url=%s", topic, url)
		fmt.Println(httpUrl)
		resp, err := misc.MakeRequest(httpUrl)
		if err != nil {
			fmt.Println(err)
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

