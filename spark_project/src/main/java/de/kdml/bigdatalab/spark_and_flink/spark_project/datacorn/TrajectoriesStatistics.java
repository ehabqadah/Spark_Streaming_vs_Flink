package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.StatisticsUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.TrajectoriesUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;

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

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

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

		printLatencies(runningTrajectories);

		runningTrajectories.print(1000);

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("Error:" + e.getMessage());
		}
	}

	/**
	 * Print latencies measurements
	 * 
	 * @param runningTrajectories
	 */
	private static void printLatencies(JavaPairDStream<String, Iterable<PositionMessage>> runningTrajectories) {
		JavaDStream<Long> latencies = runningTrajectories.map(trajectoryTuple -> {

			long currentTime = System.currentTimeMillis(), sumLatency = 0, count = 0;

			Iterable<PositionMessage> trajectoriesList = trajectoryTuple._2;
			for (PositionMessage trajectory : trajectoriesList) {
				if (trajectory.isNew()) {
					sumLatency += currentTime - trajectory.getStreamedTime();
					count++;
				}
			}
			return count == 0 ? 0 : new Long(sumLatency / count);

		}).filter(latency -> {
			return latency > 0;
		});

		// latencies.print();
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

			// Get last old trajectory to be used in statistics computation for
			// new batch positions

			Iterable<PositionMessage> prevBatchPositions = statePositions.orElse(new ArrayList<>());

			// Get last position in previous batch
			PositionMessage lastOldPosition = getLastPosition(prevBatchPositions);

			// Combine all new batch positions
			List<PositionMessage> aggregatedPositions = new ArrayList<>();
			// aggregate new values
			for (Iterable<PositionMessage> val : newPostitions) {
				for (PositionMessage position : val) {
					aggregatedPositions.add(position);
				}

			}

			// In case there is no new positions keep the current state for the
			// given trajectory
			if (aggregatedPositions.size() == 0) {
				return Optional.of(prevBatchPositions);
			}

			// First, use the last old position as the first record for
			// calculating the statistics quantities, and the sort is needed
			// since we need to preserve the right order of the position
			// messages based on the streamed time
			aggregatedPositions = TrajectoriesUtils.sortTrajectories(aggregatedPositions);

			for (PositionMessage trajectory : aggregatedPositions) {
				// Compute statistics for each position message based on
				// the aggregated values of previous position statistics
				StatisticsUtils.computeStatistics(lastOldPosition, trajectory);
				lastOldPosition = trajectory;

			}
			// Aggregate state
			return Optional.of(aggregatedPositions);
		}

		/**
		 * Get last position message in previous batch's positions
		 */
		private PositionMessage getLastPosition(Iterable<PositionMessage> prevBatchPositions) {
			PositionMessage lastOldPosition = null;
			for (PositionMessage trajectory : prevBatchPositions) {
				lastOldPosition = trajectory;
				lastOldPosition.setNew(false);
			}
			return lastOldPosition;
		}
	};

}
