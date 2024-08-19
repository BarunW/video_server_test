package misc

import "time"

func NewCamId() int{
    return int(time.Now().UnixMicro())
}
