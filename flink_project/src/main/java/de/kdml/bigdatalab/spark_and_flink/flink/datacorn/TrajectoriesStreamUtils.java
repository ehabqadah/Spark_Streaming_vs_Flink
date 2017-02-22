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
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.StreamRecord;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;

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
	 * Get trajectories stream
	 * 
	 * @param env
	 * @return
	 */
	public static KeyedStream<Tuple2<String, PositionMessage>, Tuple> getTrajectoriesStream(StreamExecutionEnvironment env) {

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

		DataStream<String> dataLines = env.addSource(kafkaConsumer);

		/**
		 * 1- map each data line to Tuple2<id,list of Trajectories>
		 * 
		 * 2-Filter messages based on type 3 &2
		 * 
		 * 3- keyBy- Logically partitions a stream into disjoint partitions,
		 * each partition containing elements of the same key (0-> trajectory
		 * ID) (keyBy Internally is implemented using Partitioner based on the
		 * hash value of a key)
		 * 
		 * 4- Group the same trajectories using the reduce function on
		 * KeyedStream reduce(Applies a reduce transformation on the grouped
		 * data stream grouped on by the given key position. it receives input
		 * values based on the key value. Only input values with the same key
		 * will go to the same reducer.)
		 * 
		 **/

		KeyedStream<Tuple2<String, PositionMessage>, Tuple> trajectoriesStream = dataLines.map(line -> {

			StreamRecord streamRecord = StreamRecord.parseData(line);
			PositionMessage trajectory = TrajectoriesUtils.parseDataInput(streamRecord.getValue());
			trajectory.setStreamedTime(streamRecord.getStreamedTime());
			trajectory.setNew(true);
			return new Tuple2<>(trajectory.getID(), trajectory);

		}).filter(tuple -> {

			return tuple.f1 != null && ("MSG2".equals(tuple.f1.getType()) || "MSG3".equals(tuple.f1.getType()));

		}).keyBy(0);

		return trajectoriesStream;
	}
}
