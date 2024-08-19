package models

type Camera struct {
	Name          string
	ConnectionURL string
}


const (
    PUB_ON_TOPIC = iota
    PUB_ON_PARTITION 
)
