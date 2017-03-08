package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import de.kdml.bigdatalab.spark_and_flink.common_utils.LoggerUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.StatisticsUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
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
		LoggerUtils.logMessage("Start Time:" + System.currentTimeMillis());
		/**
		 * Reduce the trajectories by aggregate the statics from the previous
		 * trajectory record and keep the new record, the further aggregation
		 * and discard old record.
		 **/

		DataStream<Tuple2<String, PositionMessage>> trajectoriesStream = TrajectoriesStreamUtils
				.getTrajectoriesStream(env)
				.reduce((Tuple2<String, PositionMessage> tuple1, Tuple2<String, PositionMessage> tuple2) -> {
					// compute & aggregate statistics for the new trajectory
					// based on the statistics of old trajectory
					// discard the old trajectory and just keep the new one
					Tuple2<String, PositionMessage> oldTuple = tuple2.f1.getStreamedTime() < tuple1.f1.getStreamedTime()
							? tuple2 : tuple1;
					Tuple2<String, PositionMessage> newTuple = tuple2.f1.getStreamedTime() < tuple1.f1.getStreamedTime()
							? tuple1 : tuple2;

					// to count first tuple in the throughput
					// if (oldTuple.f1.getSpeed() == null) {

					// LoggerUtils.logMessage("-" + System.currentTimeMillis());
					// }

					StatisticsUtils.computeStatistics(oldTuple.f1, newTuple.f1);
					newTuple.f1.setNew(false);

					LoggerUtils.logMessage("-" + System.currentTimeMillis());
					return newTuple;
				});

		// TrajectoriesStreamUtils.showLatecies(trajectoriesStream);
		trajectoriesStream.print().setParallelism(1);

		// showThroughput(trajectoriesStream);
		env.execute(" Flink Trajectories Statistics Computation");
	}

}
