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
 * Compute statistics per trajectory
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

		// Start the computation

		JavaPairDStream<String, Iterable<PositionMessage>> trajectories = TrajectoriesStreamUtils
				.getTrajectoriesStream(jssc);
		// update the stream state
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
	 * Return a new "state" DStream where the state for each key is updated by
	 * applying the given function on the previous state of the key and the new
	 * values for the key
	 * 
	 */
	public static Function2<List<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>> updateTrajectoriesAndComputeStatistics = new Function2<List<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>>() {

		private static final long serialVersionUID = -76088662409004569L;

		@Override
		public Optional<Iterable<PositionMessage>> call(List<Iterable<PositionMessage>> values,
				Optional<Iterable<PositionMessage>> state) {

			PositionMessage lastOldTrajectory = null;
			List<PositionMessage> aggregatedTrajectories = new ArrayList<>();
			// get last old trajectory to be used in statistics computation
			Iterable<PositionMessage> stateTrajectories = state.orElse(new ArrayList<>());
			for (PositionMessage trajectory : stateTrajectories) {
				lastOldTrajectory = trajectory;
				lastOldTrajectory.setNew(false);
			}
			
			// aggregate new values
			for (Iterable<PositionMessage> val : values) {
				for (PositionMessage trajectory : val) {
					aggregatedTrajectories.add(trajectory);
				}

			}
			if(aggregatedTrajectories.size()==0)
			{
				return Optional.of(stateTrajectories);
			}
			
			aggregatedTrajectories = TrajectoriesUtils.sortTrajectories(aggregatedTrajectories);

			for (PositionMessage trajectory : aggregatedTrajectories) {
				// compute statistics for each trajectory
				StatisticsUtils.computeStatistics(lastOldTrajectory, trajectory);
				lastOldTrajectory = trajectory;

			}
			// update state
			return Optional.of(aggregatedTrajectories);
		}
	};

	/**
	 * NOTE: mapWithState is still in the experimental phase
	 * JavaPairDStream<String, Iterable<Trajectory>> runningTrajectories =
	 * trajectories
	 * .mapWithState(StateSpec.function(updateTrajectoriesStreamFunction2)).stateSnapshots();
	 * 
	 * * Return a new "state" DStream where the state for each key is updated by
	 * applying the given function on the previous state of the key and the new
	 * values for the key
	 * 
	 * To be used with mapWithState
	 * 
	 * public static Function3<String, Optional<Iterable<Trajectory>>,
	 * State<Iterable<Trajectory>>, Tuple2<String, Iterable<Trajectory>>>
	 * updateTrajectoriesStreamFunction2 = new Function3<String,
	 * Optional<Iterable<Trajectory>>, State<Iterable<Trajectory>>,
	 * Tuple2<String, Iterable<Trajectory>>>() {
	 * 
	 * private static final long serialVersionUID = -1393453967261881632L;
	 * 
	 * @Override public Tuple2<String, Iterable<Trajectory>> call(String id,
	 *           Optional<Iterable<Trajectory>> values,
	 *           State<Iterable<Trajectory>> state) throws Exception {
	 *           List<Trajectory> aggregatedTrajectories = new ArrayList<>(); //
	 *           add old state for (Trajectory trajectory : (state.exists() ?
	 *           state.get() : new ArrayList<Trajectory>())) {
	 *           aggregatedTrajectories.add(trajectory); }
	 * 
	 *           // aggergate new values
	 * 
	 *           for (Trajectory val : values.orElse(new
	 *           ArrayList<Trajectory>())) {
	 * 
	 *           aggregatedTrajectories.add(val);
	 * 
	 *           }
	 * 
	 *           aggregatedTrajectories =
	 *           TrajectoriesUtils.sortTrajectories(aggregatedTrajectories);
	 *           state.update(aggregatedTrajectories); return new Tuple2<>(id,
	 *           aggregatedTrajectories); } };
	 */
}
