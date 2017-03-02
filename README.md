# Comparative Evaluation of Spark and Flink Stream Processing


## Kafka cluster setup
* download Kafka [here](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz)

* Un-tar the downloaded file: tar -xzf kafka_2.11-0.10.2.0.tgz && cd kafka_2.11-0.10.2.0
* Start a ZooKeeper server: sudo bin/zookeeper-server-start.sh config/zookeeper.properties

* Edit the Kafka servers config file once:
 * cp config/server.properties config/server-1.properties
 * edit server-1.properties and update: broker.id=1, port=9092, log.dir=/tmp/kafka-logs-1, listeners=PLAINTEXT://:9092
 * cp config/server.properties config/server-2.properties
 * edit server-1.properties and update: broker.id=2, port=9093, log.dir=/tmp/kafka-logs-2, listeners=PLAINTEXT://:9093  
* Start server1: sudo bin/kafka-server-start.sh config/server-1.properties
* Start server2: sudo bin/kafka-server-start.sh config/server-2.properties
* Create datacorn topic: sudo bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic datacorn
* Producer console: sudo bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093 --topic datacorn
* Conumer console: sudo ./bin/kafka-console-consumer.sh --topic datacorn --bootstrap-server localhost:9092,localhost:9093
* Delete topic command: sudo bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic datacorn

## Setup in  development mode

1. Change to the project directory
2. run:  `mvn clean package`
3. use eclipes to run code.
