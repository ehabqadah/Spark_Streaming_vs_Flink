package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.TrajectoriesUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.StreamRecord;

/**
 * Initialize the trajectories stream
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */

public class TrajectoriesStreamUtils {

	private static Configs configs = Configs.getInstance();

	/**
	 * Get the trajectories stream
	 */

	public static KeyedStream<Tuple2<String, PositionMessage>, Tuple> getTrajectoriesStream(
			StreamExecutionEnvironment env) {

		// configure Kafka consumer
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", configs.getStringProp("zookeeper"));
		props.setProperty("bootstrap.servers", configs.getStringProp("bootstrap.servers"));
		props.setProperty("group.id", configs.getStringProp("kafkaGroupId"));
		// Always read topicfrom start
		props.setProperty("auto.offset.reset", "earliest");

		// create a Kafka consumer
		FlinkKafkaConsumer09<String> kafkaConsumer = new FlinkKafkaConsumer09<>(configs.getStringProp("topicId"),
				new SimpleStringSchema(), props);
		// constructs the data stream
		DataStream<String> postitionMessagesStream = env.addSource(kafkaConsumer);

		/**
		 * 1- Map each data line to Tuple2<id,list of Trajectories>
		 * 
		 * 2-Filter messages based on type 3 &2
		 * 
		 * 3- Construct the trajectories stream using the keyBy- Logically
		 * partitions a stream into disjoint partitions, each partition
		 * containing elements of the same key (0-> trajectory ID) (keyBy
		 * Internally is implemented using Partitioner based on the hash value
		 * of a key)
		 **/

		KeyedStream<Tuple2<String, PositionMessage>, Tuple> trajectoriesStream = postitionMessagesStream.map(line -> {
			// parse ADS-B messages and construct tuple of ID & position
			// message for each message
			StreamRecord streamRecord = StreamRecord.parseData(line);
			PositionMessage trajectory = TrajectoriesUtils.parseDataInput(streamRecord.getValue());
			trajectory.setStreamedTime(streamRecord.getStreamedTime());
			trajectory.setNew(true);
			return new Tuple2<>(trajectory.getID(), trajectory);

		}).filter(tuple -> {
			// filter irrelevant messages
			return tuple.f1 != null && ("MSG2".equals(tuple.f1.getType()) || "MSG3".equals(tuple.f1.getType()));

		}).keyBy(0);// group all tuples with same ID

		return trajectoriesStream;
	}
}
