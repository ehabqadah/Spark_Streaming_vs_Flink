package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.StatisticsUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.TrajectoriesUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;
import scala.Tuple2;

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

		JavaPairDStream<String, Iterable<Trajectory>> trajectories = TrajectoriesStreamUtils
				.getTrajectoriesStream(jssc);
		// update the stream state
		JavaPairDStream<String, Iterable<Trajectory>> runningTrajectories = trajectories
				.updateStateByKey(updateTrajectoriesAndComputeStatistics);

		JavaDStream<Long> latencies = runningTrajectories.map(trajectoryTuple -> {

			long currentTime = System.currentTimeMillis(), sumLatency = 0, count = 0;

			Iterable<Trajectory> trajectoriesList = trajectoryTuple._2;
			for (Trajectory trajectory : trajectoriesList) {
				if (trajectory.isNew()) {
					sumLatency += currentTime - trajectory.getStreamedTime();
					count++;
				}
			}
			return count == 0 ? 0 : new Long(sumLatency / count);

		}).filter(latency -> {
			return latency > 0;
		});

		//latencies.print();

		runningTrajectories.print(1000);

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("Error:" + e.getMessage());
		}
	}

	/**
	 * Return a new "state" DStream where the state for each key is updated by
	 * applying the given function on the previous state of the key and the new
	 * values for the key
	 * 
	 */
	public static Function2<List<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>> updateTrajectoriesAndComputeStatistics = new Function2<List<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>>() {

		private static final long serialVersionUID = -76088662409004569L;

		@Override
		public Optional<Iterable<Trajectory>> call(List<Iterable<Trajectory>> values,
				Optional<Iterable<Trajectory>> state) {

			List<Trajectory> aggregatedTrajectories = new ArrayList<>();
			// add old state
			for (Trajectory trajectory : state.orElse(new ArrayList<>())) {
				trajectory.setNew(false);
				aggregatedTrajectories.add(trajectory);

			}

			// aggregate new values

			for (Iterable<Trajectory> val : values) {
				for (Trajectory trajectory : val) {
					trajectory.setNew(true);
					aggregatedTrajectories.add(trajectory);
				}

			}

			aggregatedTrajectories = TrajectoriesUtils.sortTrajectories(aggregatedTrajectories);
			return Optional.of(StatisticsUtils.computeStatistics(aggregatedTrajectories));
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
