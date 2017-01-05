package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;
import de.kdml.bigdatalab.spark_and_flink.flink.utils.FlinkUtils;

/**
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
public class TrajectoriesSectorChangeDetector {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = FlinkUtils.getInitializedEnv();
		DataStream<Tuple2<String, List<Trajectory>>> trajectoriesStream = TrajectoriesStreamUtils
				.getTrajectoriesStream(env);

		trajectoriesStream.print().setParallelism(1);
		env.execute("Trajectories Sector change detector");
	}
}
