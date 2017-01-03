package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.util.List;

/**
 * 
 * 
 * @author Ehab Qadah
 * 
 *         Jan 3, 2017
 */
public class StatisticsUtils {

	/**
	 * 
	 * @param trajectories
	 * @return
	 */
	public static List<Trajectory> computeStatistics(List<Trajectory> trajectories) {

		double minLongtitude = Double.MAX_VALUE;

		for (Trajectory trajectory : trajectories) {

			double longtitude = trajectory.getLongtitude();
			if (longtitude < minLongtitude) {
				minLongtitude = longtitude;
			}
		}

		TrajectoryStatisticsWrapper trajectoryStatisticsWrapper = new TrajectoryStatisticsWrapper();
		trajectoryStatisticsWrapper.setMinLong(minLongtitude);

		for (Trajectory trajectory : trajectories) {

			// just update new trajectories
			if (trajectory.getStatistics() == null) {

				trajectory.setStatistics(trajectoryStatisticsWrapper);
			}

		}

		return trajectories;
	}

}
