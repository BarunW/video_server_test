#Place holder for proto file 
FILE_NAME ?= frameData.proto

# target command
all : proto 

# to generate both .pb.go and _rpc.pb.go for the proto file 
proto: 
	protoc -I protos/ protos/$(FILE_NAME) --go_out=:. && protoc -I protos/ protos/$(FILE_NAME) --go-grpc_out=:.

fbs:
	~/flatbuffers/flatc -g --gen-object-api --go-module-name echo flat_buffers/FrameData.fbs
