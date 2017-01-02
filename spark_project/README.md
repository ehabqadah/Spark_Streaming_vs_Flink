
mvn compile
mvn assembly:single

spark-submit \
--class "de.kdml.bigdatalab.spark.App" \
--master local[4] \
target/spark_boilerplate-0.0.1-SNAPSHOT-jar-with-dependencies.jar

cd /usr/local/kafka
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
sudo bin/kafka-server-start.sh config/server.properties
sudo bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic test

sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
sudo ./bin/kafka-console-consumer.sh --topic test --zookeeper localhost:2181

#new command 
sudo ./bin/kafka-console-consumer.sh --topic test --bootstrap-server localhost:9092

mvn eclipse:eclipse
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.0.2 \
mvn clean package
spark-submit \
--class "de.kdml.bigdatalab.spark.App" \
--master local[4] \
--jars spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar \
target/spark_boilerplate-0.0.1-SNAPSHOT.jar

--jars jars/spark-streaming-kafka-0-10_2.11-2.0.2.jar,jars/spark-sql-kafka-0-10_2.11-2.0.2.jar \

--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2

