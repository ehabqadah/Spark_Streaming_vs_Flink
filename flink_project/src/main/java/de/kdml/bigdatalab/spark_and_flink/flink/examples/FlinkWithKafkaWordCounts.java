package de.kdml.bigdatalab.spark_and_flink.flink.examples;

import java.util.Properties;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import de.kdml.bigdatalab.spark_and_flink.flink.utils.FlinkUtils;

/**
 * This example reads stream of lines in Flink and process the lines to find the
 * word counts
 * 
 * @author Ehab Qadah
 * 
 *         Dec 8, 2016
 */

public class FlinkWithKafkaWordCounts {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = FlinkUtils.getInitializedEnv();

		// configure Kafka consumer
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", "localhost:2181");
		props.setProperty("bootstrap.servers", "localhost:9092,localhost:9093");
		props.setProperty("group.id", "myGroup"); // Consumer group ID
		// Always read topicfrom start
		props.setProperty("auto.offset.reset", "earliest");

		// create a Kafka consumer
		FlinkKafkaConsumer09<String> kafkaConsumer = new FlinkKafkaConsumer09<>("datacorn", new SimpleStringSchema(),
				props);

	
		// create Kafka consumer data source
		DataStream<String> messages = env.addSource(kafkaConsumer);
		
		DataStream<Tuple2<String, Integer>> counts = // split up the lines in
														// pairs (2-tuples)
														// containing: (word,1)
				messages.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
					// normalize and split the line into words
					String[] tokens = line.toLowerCase().split("\\W+");

					// emit the pairs
					for (String token : tokens) {
						if (token.length() > 0) {
							out.collect(new Tuple2<String, Integer>(token, 1));
						}
					}

				}).keyBy(0).sum(1);

		counts.print().setParallelism(1);

		// implement simple sink function
		counts.addSink(new SinkFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void invoke(Tuple2<String, Integer> value) throws Exception {

				//System.out.println(value.toString());

			}
		});

		// Trigger program execution
		env.execute("flink streaming word counts");
			}
}