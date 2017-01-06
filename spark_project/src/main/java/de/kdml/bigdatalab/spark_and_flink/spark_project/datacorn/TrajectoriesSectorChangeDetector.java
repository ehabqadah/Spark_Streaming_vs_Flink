package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.SectorUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.TrajectoriesUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;
import de.kdml.bigdatalab.spark_and_flink.spark_project.SparkConfigsUtils;

/**
 * Sector change detector in stream of trajectories
 * 
 * @author Ehab Qadah
 * 
 *         Jan 6, 2017
 */
public class TrajectoriesSectorChangeDetector {

	private static Configs configs = Configs.getInstance();

	static Broadcast<List<Sector>> sectors;

	public static void main(String[] args) {

		// configure spark streaming context
		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("Trajectories sector change  deetctor ");

		JavaStreamingContext jssc = new JavaStreamingContext(sc,
				Durations.seconds(configs.getIntProp("batchDuration")));

		// broadcast the sector list
		sectors = sc.broadcast(getSectorsDataSet(sc));
		// Start the computation

		JavaPairDStream<String, Iterable<Trajectory>> trajectories = TrajectoriesStreamUtils
				.getTrajectoriesStream(jssc);
		// update the stream state
		JavaPairDStream<String, Iterable<Trajectory>> runningTrajectories = trajectories
				.updateStateByKey(updateTrajectoriesAndAssignSectors);

		// find trajectories with change in sector
		JavaDStream<String> changedTrajectories = runningTrajectories.filter(trajectoryTuple -> {

			// filter the trajectories that contain change in sector between two
			// consecutive trajectories
			Iterable<Trajectory> trajectoriesList = trajectoryTuple._2;

			Trajectory prevTrajectory = null;
			for (Trajectory trajectory : trajectoriesList) {
				if (prevTrajectory != null) {
					if ((prevTrajectory.isNew() || trajectory.isNew())
							&& (!trajectory.getSector().equals(prevTrajectory.getSector()))) {
						return true;
					}
				}
				prevTrajectory = trajectory;
			}

			return false;
		}).map(trajectoryTuple -> {

			// print the sector change transitions
			StringBuilder output = new StringBuilder(trajectoryTuple._1);
			output.append(": ");
			Iterable<Trajectory> trajectoriesList = trajectoryTuple._2;

			Trajectory prevTrajectory = null;
			for (Trajectory trajectory : trajectoriesList) {

				if (prevTrajectory != null) {

					if ((prevTrajectory.isNew() || trajectory.isNew())
							&& (!trajectory.getSector().equals(prevTrajectory.getSector()))) {

						output.append("\n");
						output.append(prevTrajectory.getSector().getNameAndAirBlock());
						output.append("--->");
						output.append(trajectory.getSector().getNameAndAirBlock());
					}
				}
				prevTrajectory = trajectory;
			}
			return output.toString();
		});

		changedTrajectories.print();

		/**
		 * 
		 * for sectors update
		 * 
		 * // to update broadcasted rdd in the driver
		 * runningTrajectories.foreachRDD(line -> {
		 * 
		 * sectors.unpersist();
		 * 
		 * sector = ... update
		 * 
		 * });
		 **/
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			System.out.println("Error:" + e.getMessage());
		}
	}

	/**
	 * Get the sectors data
	 * 
	 * @param sc
	 * @return
	 */
	private static List<Sector> getSectorsDataSet(JavaSparkContext sc) {
		return sc.textFile(configs.getStringProp("sectorsFilePath")).map(sectorLine -> {
			return Sector.parseSectorData(sectorLine);
		}).collect();
	}

	/**
	 * Return a new "state" DStream where the state for each key is updated by
	 * applying the given function on the previous state of the key and the new
	 * values for the key
	 * 
	 */
	public static Function2<List<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>> updateTrajectoriesAndAssignSectors = new Function2<List<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>, Optional<Iterable<Trajectory>>>() {

		private static final long serialVersionUID = -76088662409004569L;

		@Override
		public Optional<Iterable<Trajectory>> call(List<Iterable<Trajectory>> values,
				Optional<Iterable<Trajectory>> state) {

			List<Trajectory> aggregatedTrajectories = new ArrayList<>();
			// add old state
			for (Trajectory trajectory : state.orElse(new ArrayList<>())) {
				trajectory.setNew(false);
				appendTrajectory(aggregatedTrajectories, trajectory);
			}

			// aggregate new values

			for (Iterable<Trajectory> val : values) {
				for (Trajectory trajectory : val) {

					trajectory.setNew(true);
					appendTrajectory(aggregatedTrajectories, trajectory);

				}

			}
			// sort trajectories
			return Optional.of(TrajectoriesUtils.sortTrajectories(aggregatedTrajectories));
		}

		/**
		 * Append the trajectory after assigning the sector
		 * 
		 * @param aggregatedTrajectories
		 * @param trajectory
		 */
		private void appendTrajectory(List<Trajectory> aggregatedTrajectories, Trajectory trajectory) {

			// assign sector for trajectory
			if (trajectory.getSector() == null) {
				Sector sector = SectorUtils.getSectorForTrajectory(trajectory, sectors.getValue());
				trajectory.setSector(sector);
			}
			aggregatedTrajectories.add(trajectory);
		}
	};

}
