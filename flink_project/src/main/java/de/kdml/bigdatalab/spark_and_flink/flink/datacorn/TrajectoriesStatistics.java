package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import java.util.Properties;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;
import de.kdml.bigdatalab.spark_and_flink.flink.utils.FlinkUtils;

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

		StreamExecutionEnvironment env = FlinkUtils.getInitializedEnv();

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
		DataStream<Tuple2<String, List<Trajectory>>> trajectoriesStream = dataLines.map(line -> {
			Trajectory trajectory = Trajectory.parseDataInput(line);
			return new Tuple2<>(trajectory.getID(), Arrays.asList(trajectory));

		}).filter(tuple -> {

			return tuple.f1.get(0) != null
					&& ("MSG2".equals(tuple.f1.get(0).getType()) || "MSG3".equals(tuple.f1.get(0).getType()));

		}).keyBy(0).reduce((Tuple2<String, List<Trajectory>> tuple1, Tuple2<String, List<Trajectory>> tuple2) -> {

			// f0 -> trajectory ID f1-> list of trajectories

			List<Trajectory> trajectories = new ArrayList<>();
			if (!tuple1.f1.isEmpty()) {
				trajectories.addAll(tuple1.f1);
			}

			if (!tuple2.f1.isEmpty()) {
				trajectories.addAll(tuple2.f1);
			}

			Tuple2<String, List<Trajectory>> reduced = new Tuple2<>(tuple1.f0, trajectories);
			return reduced;
		}).map(tuple -> {
			//TODO: add statistics computation
			return tuple;
		});
		;

		trajectoriesStream.print().setParallelism(1);

		env.execute(" Flink Trajectories Statistics Computation");
	}
}
