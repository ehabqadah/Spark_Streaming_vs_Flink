package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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
		//java 8 stream 
		Stream<Trajectory> ordered = trajectories.stream().sorted((t1, t2) -> {
			return Double.compare(t1.getLongtitude(), t2.getLongtitude());
		});
		Arrays.asList(ordered.toArray());
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
