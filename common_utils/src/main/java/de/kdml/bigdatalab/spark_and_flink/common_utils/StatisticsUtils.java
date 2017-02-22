package de.kdml.bigdatalab.spark_and_flink.common_utils;

import java.util.List;

import de.kdml.bigdatalab.spark_and_flink.common_utils.data.PositionMessage;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.TrajectoryStatisticsWrapper;

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
	public static List<PositionMessage> computeStatistics(List<PositionMessage> trajectories) {

		double minLongtitude = Double.MAX_VALUE, minLatitude = Double.MAX_VALUE, minAltitude = Double.MAX_VALUE,
				minSpeed = Double.MAX_VALUE, minAcceleration = Double.MAX_VALUE;
		double maxLongtitude = Double.MIN_VALUE, maxLatitude = Double.MIN_VALUE, maxAltitude = Double.MIN_VALUE,
				maxSpeed = Double.MIN_VALUE, maxAcceleration = Double.MIN_VALUE;

		PositionMessage prevTrajectory = null;
		for (PositionMessage trajectory : trajectories) {

			TrajectoriesUtils.calculateDistanceAndSpeedOfTrajectory(prevTrajectory, trajectory);

			double longtitude = trajectory.getLongitude(), lat = trajectory.getLatitude(),
					altit = trajectory.getAltitude(), speed = trajectory.getSpeed(),
					acceleration = trajectory.getAcceleration();
			// update min longitude
			minLongtitude = Math.min(minLongtitude, longtitude);
			// update min latitude
			minLatitude = Math.min(minLatitude, lat);
			minAltitude = Math.min(minAltitude, altit);
			minAcceleration = Math.min(minAcceleration, acceleration);
			minSpeed = Math.min(minSpeed, speed);
			// update max longitude
			maxLongtitude = Math.max(maxLongtitude, longtitude);
			// update max latitude
			maxLatitude = Math.max(maxLatitude, lat);
			maxAltitude = Math.max(maxAltitude, altit);
			maxSpeed = Math.max(maxSpeed, speed);
			maxAcceleration = Math.max(maxAcceleration, acceleration);

			prevTrajectory = trajectory;

		}

		TrajectoryStatisticsWrapper statistics = new TrajectoryStatisticsWrapper();
		statistics.setMinLong(minLongtitude);
		statistics.setMinLat(minLatitude);
		statistics.setMinAltitude(minAltitude);
		statistics.setMaxLong(maxLongtitude);
		statistics.setMaxLat(maxLatitude);
		statistics.setMaxAltitude(maxAltitude);
		statistics.setMaxAcceleration(maxAcceleration);
		statistics.setMinAcceleration(minAcceleration);
		statistics.setMaxSpeed(maxSpeed);
		statistics.setMinSpeed(minSpeed);

		for (PositionMessage trajectory : trajectories) {

			// just update new trajectories
			if (trajectory.getStatistics() == null) {

				trajectory.setStatistics(statistics);
			}

		}

		return trajectories;
	}

	/**
	 * Compute statistics by aggregating from old trajectory
	 * 
	 * @param oldTrajectory
	 * @param newTrajectory
	 */
	public static void computeStatistics(PositionMessage oldTrajectory, PositionMessage newTrajectory) {

		if (oldTrajectory == null) {
			intitStaticsForOldTrajectory(newTrajectory);
			return;
		}
		intitStaticsForOldTrajectory(oldTrajectory);// initialize statics for
													// the first trajectory only
		double minLongtitude, minLatitude, minAltitude, minSpeed, minAcceleration;
		double maxLongtitude, maxLatitude, maxAltitude, maxSpeed, maxAcceleration;
		TrajectoriesUtils.calculateDistanceAndSpeedOfTrajectory(oldTrajectory, newTrajectory);
		double longtitude = newTrajectory.getLongitude(), lat = newTrajectory.getLatitude(),
				altit = newTrajectory.getAltitude(), speed = newTrajectory.getSpeed(),
				acceleration = newTrajectory.getAcceleration();
		TrajectoryStatisticsWrapper oldStatistics = oldTrajectory.getStatistics();
		// update min longitude
		minLongtitude = Math.min(oldStatistics.getMinLong(), longtitude);
		// update min latitude
		minLatitude = Math.min(oldStatistics.getMinLat(), lat);
		minAltitude = Math.min(oldStatistics.getMinAltitude(), altit);
		minAcceleration = Math.min(oldStatistics.getMaxAcceleration(), acceleration);
		minSpeed = Math.min(oldStatistics.getMinSpeed(), speed);
		// update max longitude
		maxLongtitude = Math.max(oldStatistics.getMaxLong(), longtitude);
		// update max latitude
		maxLatitude = Math.max(oldStatistics.getMaxLat(), lat);
		maxAltitude = Math.max(oldStatistics.getMaxAltitude(), altit);
		maxSpeed = Math.max(oldStatistics.getMaxSpeed(), speed);
		maxAcceleration = Math.max(oldStatistics.getMaxAcceleration(), acceleration);

		TrajectoryStatisticsWrapper statistics = new TrajectoryStatisticsWrapper();
		statistics.setMinLong(minLongtitude);
		statistics.setMinLat(minLatitude);
		statistics.setMinAltitude(minAltitude);
		statistics.setMaxLong(maxLongtitude);
		statistics.setMaxLat(maxLatitude);
		statistics.setMaxAltitude(maxAltitude);
		statistics.setMaxAcceleration(maxAcceleration);
		statistics.setMinAcceleration(minAcceleration);
		statistics.setMaxSpeed(maxSpeed);
		statistics.setMinSpeed(minSpeed);

		newTrajectory.setStatistics(statistics);
	}

	/**
	 * Initialize the statistics of the first trajectory
	 * 
	 * @param oldTrajectory
	 */
	private static void intitStaticsForOldTrajectory(PositionMessage oldTrajectory) {
		if (oldTrajectory.isNew() || oldTrajectory.getStatistics() == null) {

			TrajectoryStatisticsWrapper statistics = new TrajectoryStatisticsWrapper();
			statistics.setMinLong(oldTrajectory.getLongitude());
			statistics.setMinLat(oldTrajectory.getLatitude());
			statistics.setMinAltitude(oldTrajectory.getAltitude());
			statistics.setMaxLong(oldTrajectory.getLongitude());
			statistics.setMaxLat(oldTrajectory.getLatitude());
			statistics.setMaxAltitude(oldTrajectory.getAltitude());
			statistics.setMaxAcceleration(0);
			statistics.setMinAcceleration(0);
			statistics.setMaxSpeed(0);
			statistics.setMinSpeed(0);

			oldTrajectory.setStatistics(statistics);
			oldTrajectory.setSpeed(0.0);
			oldTrajectory.setAcceleration(0.0);
			oldTrajectory.setDistance(0.0);
		}

	}

}
