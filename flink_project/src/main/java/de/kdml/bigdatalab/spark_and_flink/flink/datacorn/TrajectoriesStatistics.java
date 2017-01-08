package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import de.kdml.bigdatalab.spark_and_flink.common_utils.StatisticsUtils;
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

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = FlinkUtils.getInitializedEnv();

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
		DataStream<Tuple2<String, List<Trajectory>>> trajectoriesStream = TrajectoriesStreamUtils
				.getTrajectoriesStream(env).map(tuple -> {
					// compute statistics for each new trajectory
					return new Tuple2<String, List<Trajectory>>(tuple.f0, StatisticsUtils.computeStatistics(tuple.f1));

				});

		DataStream<Long> latencies = trajectoriesStream.map(tuple -> {

			long currentTime = System.currentTimeMillis();
			// get last entered item
			Trajectory trajectory = tuple.f1.get(tuple.f1.size() - 1);

			return new Long(currentTime - trajectory.getStreamedTime());

		});

		// latencies.print().setParallelism(1);
		trajectoriesStream.print().setParallelism(1);

		env.execute(" Flink Trajectories Statistics Computation");
	}
}
