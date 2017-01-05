# Comparative Evaluation of Spark and Flink Stream processing




# kafka setup 2 servers / 4 partitions
sudo bin/zookeeper-server-start.sh config/zookeeper.properties

#server1&2.properties are copied from server.properties and updated broker.id & port & log.dir parameters 
sudo bin/kafka-server-start.sh config/server1.properties  //broker.id=1 & port=9092 & log.dir=/tmp/kafka-logs-1
sudo bin/kafka-server-start.sh config/server2.properties  //broker.id=2 & port=9093 & log.dir=/tmp/kafka-logs-2
sudo bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic datacorn

sudo bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093 --topic datacorn

sudo ./bin/kafka-console-consumer.sh --topic datacorn --bootstrap-server localhost:9092,localhost:9093


#delete topic
sudo bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic datacorn