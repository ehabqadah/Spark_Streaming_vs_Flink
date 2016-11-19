
mvn compile
mvn assembly:single

spark-submit \
--class "de.kdml.bigdatalab.spark.App" \
--master local[4] \
target/spark_boilerplate-0.0.1-SNAPSHOT-jar-with-dependencies.jar

cd /usr/local/kafka
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
sudo bin/kafka-server-start.sh config/server.properties
sudo bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

mvn clean package
spark-submit \
--class "de.kdml.bigdatalab.spark.App" \
--master local[4] \
--jars spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar \
target/spark_boilerplate-0.0.1-SNAPSHOT.jar