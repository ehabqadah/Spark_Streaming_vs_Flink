package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.Properties;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;

/**
 * Compute statistics per trajectory
 * 
 * @author Ehab Qadah
 * 
 *         Jan 2, 2017
 */
public class TrajectoriesStatistics {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
		env.setParallelism(4);

		// configure Kafka consumer
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", configs.getStringProp("zookeeper"));
		props.setProperty("bootstrap.servers", configs.getStringProp("bootstrap.servers"));
		props.setProperty("group.id", "myGroup"); // Consumer group ID
		// Always read topicfrom start
		props.setProperty("auto.offset.reset", "earliest");

		// create a Kafka consumer
		FlinkKafkaConsumer09<String> kafkaConsumer = new FlinkKafkaConsumer09<>("datacorn", new SimpleStringSchema(),
				props);

		// create Kafka consumer data source
		DataStream<String> messages = env.addSource(kafkaConsumer);

		env.execute(" Flink Trajectories Statistics Computation");
	}
}
