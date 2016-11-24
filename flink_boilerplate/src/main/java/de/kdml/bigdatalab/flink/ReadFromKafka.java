package de.kdml.bigdatalab.flink;

import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		
		
		StreamExecutionEnvironment env = 
				  StreamExecutionEnvironment.getExecutionEnvironment();
				// configure event-time characteristics
				env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
				// generate a Watermark every second
				env.getConfig().setAutoWatermarkInterval(1000);

				// configure Kafka consumer
				Properties props = new Properties();
				props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper default host:port
				props.setProperty("bootstrap.servers", "localhost:9092"); // Broker default host:port
				props.setProperty("group.id", "myGroup");                 // Consumer group ID
				props.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

				// create a Kafka consumer
				FlinkKafkaConsumer09<String> consumer = 
				  new FlinkKafkaConsumer09<>(
				    "test",
				    new SimpleStringSchema(),
				    props);

				// create Kafka consumer data source
				DataStream<String> messages = env.addSource(consumer);
				
			
				messages.print();
				env.execute();
	}
}