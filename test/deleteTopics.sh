#!/bin/bash

n=0

for(( i=0; i <=$n; i++));
do 
	topic="chunk"
	sudo docker exec -it dreamy_hellman /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $topic  
done
