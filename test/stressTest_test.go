package test

import (
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func MakeRequest(url string) (*http.Response, error){
    req, err := http.NewRequest("POST", url, nil)
    if err != nil{
        return nil, err
    }
    resp, err := http.DefaultClient.Do(req)
    if err != nil  || resp == nil{
        slog.Error("Failed to make the request", "Details", err.Error())  
        return nil, err
    }
    if resp.StatusCode != http.StatusOK{
        fmt.Println(resp.StatusCode)
        return nil, err
    }

    return resp, err
}

func TestStress(t *testing.T) {
    for i := range 10 { 
        url := fmt.Sprintf("http://localhost:8000/camera/register/test10_%d?url=rtsp://localhost:8554/webcam", i) 
        resp, err := MakeRequest(url) 
        if err != nil || resp == nil{
            t.Fail()
        }
        if resp != nil{
            if resp.StatusCode != http.StatusOK{
                fmt.Println(resp.StatusCode)
                t.Fail()
            }
            resp.Body.Close() 
        }

        <-time.After(1 * time.Second)
    }
    

}
