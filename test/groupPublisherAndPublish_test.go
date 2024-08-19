package test

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"testing"
	"time"
)

func isRtspUrl(s string) (bool, error) {
    regex, err := regexp.Compile("^rtsp://*")
    if err != nil{
        fmt.Println(err)
        return false, err
    }
    return regex.Match([]byte(s)), nil
}

func MakeRequest_ (url string) (*http.Response, error) {
    req, err := http.NewRequest("POST", url, nil)
    if err != nil {
        return nil, err
    }

    resp, err := http.DefaultClient.Do(req)
    if err != nil || resp == nil {
        slog.Error("Failed to make the request", "Details", err.Error())
        return nil, err
    }

    if resp.StatusCode != http.StatusOK {
        fmt.Println(resp.StatusCode)
        return nil, err
    }

    return resp, err
}

func getRtspUrls() ([]string, error){
    f, err := os.Open("rtsp.csv")
    if err != nil{
        slog.Error("Failed to open the err", "Details", err.Error())
        return nil, err
    }
    defer f.Close()

    csvReader := csv.NewReader(f) 

    records, err := csvReader.ReadAll()

    if err != nil{
        slog.Error("Failed to open the err", "Details", err.Error())
        return  nil, err
    }

    rtspUrls := make([]string, 0)
 
    for _, row := range records{
	targetPosition := len(row)-1
        if  ok, err := isRtspUrl(row[targetPosition]); !ok{
            if err != nil{
               return nil, err 
            }
            continue
        }

        rtspUrls = append(rtspUrls, row[targetPosition])
    }


/*   return []string{
       "rtsp://localhost:8554/s1",
       "rtsp://localhost:8554/s2",
       "rtsp://localhost:8554/s3",
       "rtsp://localhost:8554/s4",
       "rtsp://localhost:8554/s5",
       "rtsp://localhost:8554/s6",
       "rtsp://localhost:8554/s7",
       "rtsp://localhost:8554/s8",
       "rtsp://localhost:8554/s9", 	     
       "rtsp://localhost:8554/s10",      
       "rtsp://localhost:8554/s11",
       "rtsp://localhost:8554/s12",       
       "rtsp://localhost:8554/s13",
       "rtsp://localhost:8554/s14",
       "rtsp://localhost:8554/s15",    
   },nil */
   return rtspUrls, nil
}


func TestGroupPublishAndPubOnPartition(t *testing.T) {

    rtspUrls, err := getRtspUrls()
    if err != nil{
        t.Fail()
    }
	
    topic := "chunk2" 
    
    for _, url := range rtspUrls[:3]{
		httpUrl := fmt.Sprintf("http://localhost:8000/camera/register-gp/%s?url=%s", topic, url)
		fmt.Println(httpUrl)
		resp, err := MakeRequest_(httpUrl)
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

