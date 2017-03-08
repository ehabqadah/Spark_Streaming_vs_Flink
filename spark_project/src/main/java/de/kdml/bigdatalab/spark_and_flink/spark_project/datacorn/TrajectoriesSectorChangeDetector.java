package de.kdml.bigdatalab.spark_and_flink.spark_project.datacorn;

import java.util.ArrayList;
import java.util.Arrays;
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
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;
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
				Durations.milliseconds(configs.getIntProp("batchDuration")));

		// broadcast the sector list
		sectors = sc.broadcast(getSectorsDataSet(sc));
		// Start the computation

		JavaPairDStream<String, Iterable<PositionMessage>> trajectories = TrajectoriesStreamUtils
				.getTrajectoriesStream(jssc);
		// update the stream state
		JavaPairDStream<String, Iterable<PositionMessage>> runningTrajectories = trajectories
				.updateStateByKey(updateTrajectoriesAndAssignSectors);

		TrajectoriesStreamUtils.printLatencies(runningTrajectories);
		// find trajectories with change in sector
		JavaDStream<String> changedTrajectories = runningTrajectories.filter(trajectoryTuple -> {

			// filter the trajectories that contain change in sector between two
			// consecutive positions
			Iterable<PositionMessage> positionsOfTrajectory = trajectoryTuple._2;

			PositionMessage prevPosition = null;
			for (PositionMessage currentPosition : positionsOfTrajectory) {
				if (prevPosition != null) {
					if ((prevPosition.isNew() || currentPosition.isNew())
							&& (!currentPosition.getSector().equals(prevPosition.getSector()))) {
						// filter a trajectory that contains change in sector
						// between two consecutive positions
						return true;
					}
				}
				prevPosition = currentPosition;
			}

			return false;
		}).map(trajectoryTuple -> {

			// print the sector change transitions
			StringBuilder output = new StringBuilder(trajectoryTuple._1);
			output.append(": ");
			Iterable<PositionMessage> positionsOfTrajetory = trajectoryTuple._2;

			PositionMessage prevPosition = null;
			for (PositionMessage currentPosition : positionsOfTrajetory) {

				if (prevPosition != null) {

					if ((prevPosition.isNew() || currentPosition.isNew())
							&& (!currentPosition.getSector().equals(prevPosition.getSector()))) {

						output.append("\n");
						output.append(prevPosition.getSector().getNameAndAirBlock());
						output.append("--->");
						output.append(currentPosition.getSector().getNameAndAirBlock());
					}
				}
				prevPosition = currentPosition;
			}

			// LoggerUtils.logMessage(output.toString());
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
	 * Update the positions of trajectory every new batch, by assigning the
	 * sectors for the received positions
	 * 
	 */
	public static Function2<List<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>> updateTrajectoriesAndAssignSectors = new Function2<List<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>, Optional<Iterable<PositionMessage>>>() {

		private static final long serialVersionUID = -76088662409004569L;

		@Override
		public Optional<Iterable<PositionMessage>> call(List<Iterable<PositionMessage>> newPostitions,
				Optional<Iterable<PositionMessage>> statePositions) {

			// Get the last old trajectory from the previous batch to be used in
			// statistics computation for the new batch positions

			Iterable<PositionMessage> prevBatchPositions = statePositions.orElse(new ArrayList<>());

			// Get last position in previous batch
			PositionMessage lastOldPosition = TrajectoriesStreamUtils.getLastPositionInBatch(prevBatchPositions);

			List<PositionMessage> newAggregatedPositions = TrajectoriesStreamUtils
					.getNewAggregatedPositions(newPostitions);

			// In case there is no new positions keep the current state for the
			// given trajectory
			if (newAggregatedPositions.size() == 0) {
				return Optional.of(Arrays.asList(lastOldPosition));
			}

			List<PositionMessage> aggregatedPositionsOfTrajectory = new ArrayList<>();

			// append only the last old position from previous batch
			if (lastOldPosition != null) {
				appendTrajectoryAndAssignSector(aggregatedPositionsOfTrajectory, lastOldPosition);
			}

			for (PositionMessage position : newAggregatedPositions) {
				position.setNew(true);
				/**
				 * We can print the change the change here and just keep the
				 * last position
				 */
				appendTrajectoryAndAssignSector(aggregatedPositionsOfTrajectory, position);
				position.setFinishProcessingTime(System.currentTimeMillis());

			}
			// update state
			return Optional.of(TrajectoriesUtils.sortPositionsOfTrajectory(aggregatedPositionsOfTrajectory));
		}

		/**
		 * Append the trajectory after assigning the sector
		 * 
		 * @param aggregatedPositionsOfTtrajectory
		 * @param position
		 */
		private void appendTrajectoryAndAssignSector(List<PositionMessage> aggregatedPositionsOfTtrajectory,
				PositionMessage position) {

			// assign sector for trajectory
			if (position.getSector() == null) {
				Sector sector = SectorUtils.getSectorForTrajectory(position, sectors.getValue());
				position.setSector(sector);
			}
			aggregatedPositionsOfTtrajectory.add(position);
		}
	};

}
