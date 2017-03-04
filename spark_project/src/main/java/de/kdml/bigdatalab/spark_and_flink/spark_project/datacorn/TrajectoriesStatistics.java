package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Iterators;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.LoggerUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.StatisticsUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;
import scala.Tuple2;

/**
 * 
 * Compute statistics per trajectory in Spark Streaming
 * 
 * @author Ehab Qadah
 * 
 *         Jan 2, 2017
 */

public class TrajectoriesStatistics {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) {

		// configure spark streaming context
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Trajectories Statistics Computation ");

		long batchTime = configs.getIntProp("batchDuration");
		// configure the stream batch interval
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.milliseconds(batchTime));

		// create the trajectories stream
		JavaPairDStream<String, Iterable<PositionMessage>> trajectories = TrajectoriesStreamUtils
				.getTrajectoriesStream(jssc);
		/**
		 * Manage the state of the trajectories stream by calculating the
		 * statistics in every batch from aggregated old statistics, for more
		 * details check {updateTrajectoriesAndComputeStatistics}
		 */
		JavaPairDStream<String, Iterable<PositionMessage>> runningTrajectories = trajectories
				.updateStateByKey(updateTrajectoriesAndComputeStatistics);

		// printLatencies(runningTrajectories);

		runningTrajectories.print(1000);

		showThroughput(batchTime, runningTrajectories);
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("Error:" + e.getMessage());
		}
	}

	private static void showThroughput(long batchTime,
			JavaPairDStream<String, Iterable<PositionMessage>> runningTrajectories) {
		runningTrajectories.foreachRDD((rdd, time) -> {

			double batchTimeInSeconds = configs.getDoubleProp("batchDuration") / 1000.0;
			if (rdd.isEmpty()) {
				return;
			}
			List<Tuple2<String, Iterable<PositionMessage>>> tuples = rdd.collect();

			double count = 0;
			for (Tuple2<String, Iterable<PositionMessage>> tuple : tuples) {
				count += Iterators.size(tuple._2().iterator());
			}

			LoggerUtils.logMessage(time + " count=" + count + "Current throughput = " + (count / batchTimeInSeconds)
					+ " records / second");
		});
	}

	/**
	 * Print latencies measurements
	 * 
	 * @param runningTrajectories
	 */
	private static void printLatencies(JavaPairDStream<String, Iterable<PositionMessage>> runningTrajectories) {
		JavaDStream<Long> latencies = runningTrajectories.map(trajectoryTuple -> {

			// find the average latency for all positions in a trajectory
			long currentTime = System.currentTimeMillis(), sumLatency = 0, count = 0;

			Iterable<PositionMessage> positionsInTrajectory = trajectoryTuple._2;
			for (PositionMessage position : positionsInTrajectory) {
				if (position.isNew()) {

					long latency = currentTime - position.getStreamedTime();
					LoggerUtils.logMessage(String.valueOf(latency));
					sumLatency += latency;
					count++;
				}
			}
			return count == 0 ? 0 : new Long(sumLatency / count);

		}).filter(latency -> {
			return latency > 0;
		});

		// show latencies
		latencies.print();
	}

	/**
	 * Return a new "state" trajectories DStream where the state for each
	 * trajectory key is updated by applying statistics calculation and
	 * aggregation on the previous state of the key and the new values for the
	 * key
	 * 
	 */
	public static Function2<List<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>> updateTrajectoriesAndComputeStatistics = new Function2<List<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>>() {

		private static final long serialVersionUID = -76088662409004569L;

		@Override
		public Optional<Iterable<PositionMessage>> call(List<Iterable<PositionMessage>> newPostitions,
				Optional<Iterable<PositionMessage>> statePositions) {

			// Get the last old trajectory from the previous batch to be used in
			// statistics computation for the new batch positions

			Iterable<PositionMessage> prevBatchPositions = statePositions.orElse(new ArrayList<>());

			// Get last position in previous batch
			PositionMessage lastOldPosition = TrajectoriesStreamUtils.getLastPositionInBatch(prevBatchPositions);

			List<PositionMessage> aggregatedPositions = TrajectoriesStreamUtils
					.getNewAggregatedPositions(newPostitions);

			// In case there is no new positions keep the current state for the
			// given trajectory
			if (aggregatedPositions.size() == 0) {
				return Optional.of(Arrays.asList(lastOldPosition));
			}

			// First, use the last old position as the first record for
			// calculating the statistics quantities, and the sort is needed
			// since we need to preserve the right order of the position
			// messages based on the created time

			for (PositionMessage trajectory : aggregatedPositions) {
				// Compute statistics for each position message based on
				// the aggregated values of previous position statistics
				StatisticsUtils.computeStatistics(lastOldPosition, trajectory);
				lastOldPosition = trajectory;

				/**
				 * Alternative solution: print or save the positions here and
				 * save the last position in the trajectory state
				 */
			}
			// save the state
			return Optional.of(aggregatedPositions);
		}

	};

}
