package test

import (
	"fmt"
	"net/http"
	"testing"
	"time"
    "video_server/misc"
)

func TestPublishOnPartition(t *testing.T) {
    fmt.Println("Running PublishOnPartition Test")
    rtspUrls, err := misc.GetRtspUrls()
    if err != nil{
        t.Fail()
    }
	
    topic := "ligma30" 
    
    for _, url := range rtspUrls{
		httpUrl := fmt.Sprintf("http://localhost:8000/camera/register-partition/%s?url=%s", topic, url)
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


