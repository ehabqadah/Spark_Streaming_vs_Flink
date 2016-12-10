package de.kdml.bigdatalab.flink;

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

/**
 * This example reads stream of lines in Flink and process the lines to find the
 * word counts
 * 
 * @author Ehab Qadah
 * 
 *         Dec 8, 2016
 */

public class KafkaWordCounts {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		// configure event-time characteristics
		env.getConfig().disableSysoutLogging();

		// n case of a failure the system tries to restart the job 4 times and
		// waits 10 seconds in-between successive restart attempts.
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// generate a Watermark every second
		env.getConfig().setAutoWatermarkInterval(1000);

		// Enables checkpointing for the streaming job. The distributed state of
		// the streaming dataflow will be periodically snapshotted
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.setParallelism(3);

		// configure Kafka consumer
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", "localhost:2181"); // Zookeeper
																	// default
																	// host:port
		props.setProperty("bootstrap.servers", "localhost:9092"); // Broker
																	// default
																	// host:port
		props.setProperty("group.id", "myGroup"); // Consumer group ID
		props.setProperty("auto.offset.reset", "earliest"); // Always read topic
															// from start

		// create a Kafka consumer
		FlinkKafkaConsumer09<String> kafkaConsumer = new FlinkKafkaConsumer09<>("test", new SimpleStringSchema(),
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

			@Override
			public void invoke(Tuple2<String, Integer> value) throws Exception {

				System.out.println(value.toString());

			}
		});

		// Trigger program execution
		env.execute("kafka word counts");
	}
}