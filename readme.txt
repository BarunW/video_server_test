##########################
#    VIDEO SERVER TEST   #
##########################
    YOU SKIP YOU YAP 

------------------
|  Installing Go |
------------------
    - Go Version = ` 1.23 `
    - All go version can be download from here = ` https://go.dev/dl/ `
    - Follow this instruction = ` https://go.dev/doc/install `

-----------------------
| Running the project |
-----------------------
    - ` go build . ` [ for building the binary ]
    - ` go run . `   [ for running the project ] 

-------------------
|    Testing      |
-------------------
    # Make sure you run kafka and this project 
    # Have the list of rtsp url that you want to slice up the frame and push to kafka
    # make a file with the name `/video_server_test/rtsp.csv` and put all the rstp url in this file
        
    =========================
    BRIEF ARCHITECTURE 
    ========================
    #. We used HTTP REQUEST for registering the camera rtsp url
    #. 3 Types of publishing the splitted frame to kafka 
        |
        |--- ONE RTSP-URL ONE TOPIC                                 
                                    ||-> ` http://localhost:8000/camera/register/<TOPIC NAME>?url=<RTSP_URL> ` 
        |
        |--- ONE RTSP-URL ONE PARTITION OF A TOPIC [ topic and parition can be control by the consumer when ]
                                    ||-> ` http://localhost:8000/camera/register-partition/<TOPIC NAME>?url=<RTSP_URL> ` 
                    
        | 
        |--- GROUP The RTSP-URL AND ASSIGN A PARTITION OF A TOPIC [ HERE WE USE 10 Partition and a group consists of 3 rtsp url]
                                    ||-> ` http://localhost:8000/camera/register-gp/<TOPIC NAME>?url=<RTSP_URL>"  

    #. FFMPEG for splitting the frame and each request have will spin up sperate go rountine to handle this splitting 
        we are writing the frame in memory until the all the tcp packetes make a complete frame and publish to kafka
            |-> stream package is responsible for splitting the packet  
            |-> kafka_client package is responsible for publishing

    #. we use kafka message header for meta-data e.g camera-id there is no serialization of data 
    
## Use this above url for testing [ reference  tests dir/folder ] 
    
    ---------------------------------- GOOD LUCK ---------------------------------
    
            
