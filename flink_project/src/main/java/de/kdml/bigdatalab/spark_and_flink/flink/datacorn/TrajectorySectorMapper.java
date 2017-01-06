package de.kdml.bigdatalab.spark_and_flink.flink.datacorn;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import de.kdml.bigdatalab.spark_and_flink.common_utils.SectorUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.TrajectoriesUtils;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Sector;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.Trajectory;

/**
 * Trajectory and Sector maper
 * 
 * @author Ehab Qadah
 * 
 *         Jan 5, 2017
 */
final class TrajectorySectorMapper
		implements MapFunction<Tuple2<String, List<Trajectory>>, Tuple2<String, List<Trajectory>>> {

	private static final long serialVersionUID = -4643101889705396118L;
	public static List<Sector> sectors = null;

	public TrajectorySectorMapper() {
	}

	public TrajectorySectorMapper(List<Sector> sectorsList) {

		sectors = sectorsList;
	}

	/**
	 * Sort the trajectories
	 * 
	 * Assign the corresponding sector
	 * 
	 */
	@Override
	public Tuple2<String, List<Trajectory>> map(Tuple2<String, List<Trajectory>> tuple) throws Exception {

		// sort trajectories
		List<Trajectory> trajectories = TrajectoriesUtils.sortTrajectories(tuple.f1);
		// assign sector for all trajectories
		for (Trajectory trajectory : trajectories) {

			if (trajectory.getSector() == null) {
				Sector sector = SectorUtils.getSectorForTrajectory(trajectory, sectors);
				trajectory.setSector(sector);
			}

		}
		return new Tuple2<String, List<Trajectory>>(tuple.f0, trajectories);

	}
}