package misc 

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
)

func isRtspUrl(s string) (bool, error){
    regex, err := regexp.Compile("^rtsp://*")
    if err != nil{
        fmt.Println(err)
        return false, err
    }
    return regex.Match([]byte(s)), nil
}

func MakeRequest(url string) (*http.Response, error){ 
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

func GetRtspUrls() ([]string, error){
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
   },nil */
   return rtspUrls, nil
}


