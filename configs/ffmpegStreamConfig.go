package configs

import ffmpeg_go "github.com/u2takey/ffmpeg-go"

type FFMPEG_RstpStreamConfig struct {
	OutputConfig   ffmpeg_go.KwArgs
	InputConfig    ffmpeg_go.KwArgs
	ConnURL        string
	OutputFileName string
}

func NewFFMPEG_RTSPStreamConfig(connURL string) FFMPEG_RstpStreamConfig {
	fc := FFMPEG_RstpStreamConfig{}
	fc.ConnURL = connURL
	fc.OutputConfig = ffmpeg_go.KwArgs{
		// image format
		"format": "image2pipe",
		// video codec
		"vcodec": "mjpeg",
		// frame per second
		"r": "5",
		//
		"q": "4",
	}

	fc.OutputFileName = "pipe:1"
	fc.InputConfig = ffmpeg_go.KwArgs{
		//        "hwaccel" : "qsv",
		//        "c:v" : "h264_qsv",
		// 	   "loglevel" : "debug",
		"rtsp_transport": "tcp",
	}

	return fc
}
